library(plyr)
library(dplyr)
library(stringr)
library(caret)
library(xgboost)
library(doParallel)
library(parallel)

rm(list = ls())

doParallel = TRUE

if (doParallel) {
  nCores = detectCores() - 2
  cl <- makeCluster(nCores)
  registerDoParallel(cl)
  opts <- list(preschedule=TRUE)
}

setwd("c:/McKenna/Booz Allen/EcoSystem/BostonDataChallenge/Fire/")

######## Generic Functions #########

remove_whitespace           <- function(x) {return(gsub("\\s", "", x))}
remove_nonNumeric           <- function(x) {return(gsub("[[:alpha:]]*", "", x))}
remove_hyphen               <- function(x) {return(gsub("-", "", x))}
convert_dateAlarm           <- function(x) {return(as.Date(x, "%m/%d/%y"))}
convert_dateViol            <- function(x) {return(as.Date(x, "%m/%d/%Y %H:%M"))}

# Function to normalize vector between 0 and 1
normalize <- function(x) {
  (x - min(x))/(max(x) - min(x))
}

# Load BFD data and cat them all together
if (1) {
  fnames <- list("2012-bostonfireincidentopendata.csv", 
                 "2013-bostonfireincidentopendata.csv",
                 "2014-bostonfireincidentopendata.csv",
                 "2015-bostonfireincidentopendata.csv",
                 "2016-bostonfireincidentopendata.csv",
                 "january.2017-bostonfireincidents.csv",
                 "february.2017-bostonfireincidents.csv",
                 "march.2017-bostonfireincidents.csv")

  df <- data.frame()
  
  for (i in 1:length(fnames)) {
    df <- rbind(df, read.csv(fnames[[i]], na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE))
  }
  save(df, file="FireData.RData")
} else {
  load("FireData.RData")
}

# Quick fix for Property.Use
df$Property.Use <- remove_whitespace(df$Property.Use)
df$Property.Use[df$Property.Use==""] <- "0"

# Extract reports related to fire or safety depending on what we want to model
df_fireRelated <- filter(df, (Incident.Type >= 111 & Incident.Type <= 118) | # Structure fire
                             (Incident.Type >= 120 & Incident.Type <= 123) | # Fire in mobile property used as a fixed structure
                             (Incident.Type >= 151 & Incident.Type <= 164) | # Outside rubbish or special outside fire
                             (Incident.Type == 100)                        | # Fire, other
                             (Incident.Type >= 200 & Incident.Type <= 231) | # Overpressure rupture, explosion, overheat (no fire)
                             (Incident.Type >= 411 & Incident.Type <= 463))  # Hazardous condition (no fire) 

#df_fireRelated <- filter(df, (Incident.Type >= 111 & Incident.Type <= 118) | # Structure fire
#                             (Incident.Type == 100))                         # Fire, other

# Quick fixes
df_fireRelated$Street.Name   <- remove_whitespace(tolower(df_fireRelated$Street.Name))
df_fireRelated$Street.Suffix <- remove_whitespace(tolower(df_fireRelated$Street.Suffix))

# Load ISD violation data
df_code <- read.csv("cepviolations.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)

# Quick fixes
df_code$Street <- remove_whitespace(tolower(df_code$Street))
df_code$Suffix <- remove_whitespace(tolower(df_code$Suffix))
df_code$StHigh <- remove_whitespace(df_code$StHigh)
df_code$StNo   <- remove_nonNumeric(df_code$StNo)
df_code$StHigh <- remove_nonNumeric(df_code$StHigh)
df_code$StHigh[df_code$StHigh==""] <- NA
df_code$StNo   <- as.integer(df_code$StNo)
df_code$StHigh <- as.integer(df_code$StHigh)
df_code$StNo[is.na(df_code$StNo)] <- 0

# Fill in property ranges
# Done once, in theory, for code violation data
if (1) {
  # Determine size for preallocation
  idx <- which(!is.na(df_code$StHigh))
  s <- 0
  for (i in 1:length(idx)) {
    a <- df_code$StNo[idx[i]]
    b <- df_code$StHigh[idx[i]]
    if (a > b) {
      c <- a
      a <- b
      b <- c
    }
    N <- (b-1) - (a+1) + 1
    if (N <= 9) { # Fill gaps of 10 or less  
      s <- s + N
    }
  }
  
  # Preallocate
  df_code_add <- data.frame(matrix(data = NA, nrow = s, ncol = length(colnames(df_code))))
  names(df_code_add) <- names(df_code)
  
  m <- 1
  for (i in 1:length(idx)) {
    if (as.integer(i/1000) == i/1000) {
      print(i)
    }
    a <- df_code$StNo[idx[i]]
    b <- df_code$StHigh[idx[i]]
    if (a > b) {
      c <- a
      a <- b
      b <- c
    }
    N <- (b-1) - (a+1) + 1
    if ((N > 0) & (N <= 9)) {
      for (j in seq((a+1), (b-1), 1)) {
        foo <- df_code[idx[i],]
        foo$StNo <- j
        df_code_add[m,] <- foo
        m <- m + 1
      }
    }
  }
  df_code$StNo <- as.character(df_code$StNo)
  df_code_aug  <- rbind(df_code, df_code_add) # Add these fill-ins to the base code data
  save(df_code_aug, file="AugmentedCodeDataSet.RData")
} else {
  load("AugmentedCodeDataSet.RData")
}

# Features that were thought to be relevant - in the end, not all used (they get dropped)
assessFeatures = list("ST_NUM", "ST_NAME", "ST_NAME_SUF", "PTYPE", "LU", "OWN_OCC", "AV_BLDG", "YR_BUILT",
                      "STRUCTURE_CLASS", "R_BLDG_STYLE", "R_EXT_FIN", "R_HEAT_TYPE",
                      "S_BLDG_STYLE", "S_EXT_FIN", "U_HEAT_TYPE", "GROSS_AREA", "LIVING_AREA", "AV_BLDG")

# Load additional Assessment data on property structures
if (1) {
  df_bldgs14 <- read.csv("property-assessment-fy2014.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)
  df_bldgs15 <- read.csv("property-assessment-fy2015.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)
  df_bldgs16 <- read.csv("property-assessment-fy2016.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)
  df_bldgs17 <- read.csv("property-assessment-fy2017.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)
  
  # Subset assessment data to relevant features
  df_bldgs14 <- df_bldgs14 <- df_bldgs14[, colnames(df_bldgs14) %in% assessFeatures]
  df_bldgs15 <- df_bldgs15 <- df_bldgs15[, colnames(df_bldgs15) %in% assessFeatures]
  df_bldgs16 <- df_bldgs16 <- df_bldgs16[, colnames(df_bldgs16) %in% assessFeatures]
  df_bldgs17 <- df_bldgs17 <- df_bldgs17[, colnames(df_bldgs17) %in% assessFeatures]
  
  # Cat the data sets together
  foo      <- anti_join(df_bldgs16, df_bldgs17, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  df_bldgs <- rbind(df_bldgs17, foo)
  foo      <- anti_join(df_bldgs15, df_bldgs  , by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  df_bldgs <- rbind(df_bldgs, foo)
  foo      <- anti_join(df_bldgs14, df_bldgs  , by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  df_bldgs <- rbind(df_bldgs, foo)
  
  # Quick fixes
  df_bldgs$ST_NUM      <- remove_nonNumeric(tolower(df_bldgs$ST_NUM))
  df_bldgs$ST_NUM      <- remove_hyphen(df_bldgs$ST_NUM)
  df_bldgs$ST_NAME     <- remove_whitespace(tolower(df_bldgs$ST_NAME))
  df_bldgs$ST_NAME_SUF <- remove_whitespace(tolower(df_bldgs$ST_NAME_SUF))
  save(df_bldgs, file = "AssessData.RData")
} else {
  load("AssessData.RData")
}

# Fill in property ranges
# Done once, in theory, for building assessment data
if (1) {
  # Determine size for pre-allocation
  s <- 0
  for (j in 1:length(df_bldgs$ST_NUM)) {
    foo <- strsplit(df_bldgs$ST_NUM[j], " +")
    if (length(foo[[1]]) > 1) {
      a <- as.integer(foo[[1]][1])
      b <- as.integer(foo[[1]][2])
      if (is.finite(a) & is.finite(b)) {
        if (a > b) {
          c <- a
          a <- b
          b <- c
        }
        N <- (b-1) - (a+1) + 1
        if (N <= 9) { # Fill gaps of 10 or less   
          s <- s + N
        }
      }
    }
  }
  
  # Pre-allocate
  df_bldgs_add <- data.frame(matrix(data = NA, nrow = s, ncol = length(colnames(df_bldgs))))
  names(df_bldgs_add) <- names(df_bldgs)
  
  m <- 1
  for (i in 1:length(df_bldgs$ST_NUM)) {
    if (as.integer(i/1000) == i/1000) {
      print(i)
    }
    foo <- strsplit(df_bldgs$ST_NUM[i], " +")
    if (length(foo[[1]]) > 1) {
      a <- as.integer(foo[[1]][1])
      b <- as.integer(foo[[1]][2])
      if (is.finite(a) & is.finite(b)) {
        if (a > b) {
          c <- a
          a <- b
          b <- c
        }
        N <- (b-1) - (a+1) + 1
        if ((N > 0) & (N <= 9)) {
          for (j in seq((a+1), (b-1), 1)) {
            foo <- df_bldgs[idx[i],]
            foo$StNo <- j
            df_bldgs_add[m,] <- foo
            m <- m + 1
          }
        }
      }
    }
  }
  df_bldgs_add$ST_NUM <- as.character(df_bldgs_add$ST_NUM)
  df_bldgs_aug        <- rbind(df_bldgs, df_bldgs_add) # Add these fill-ins to the base bldg data
  save(df_bldgs_aug, file = "AugmentedBldgDataSet.RData")
} else {
  load("AugmentedBldgDataSet.RData")
}

# Standardize variable names
colnames(df_code_aug)[7]     <- "ST_NUM"
colnames(df_code_aug)[9]     <- "ST_NAME"
colnames(df_code_aug)[10]    <- "ST_NAME_SUF"
colnames(df_fireRelated)[15] <- "ST_NUM"
colnames(df_fireRelated)[17] <- "ST_NAME"
colnames(df_fireRelated)[19] <- "ST_NAME_SUF"

# Merge code violation data with building assessment data
df_bldgData <- inner_join(df_code_aug, df_bldgs_aug, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))

# Make dates standard
df_fireRelated$Alarm.Date <- convert_dateAlarm(df_fireRelated$Alarm.Date)
df_bldgData$Status_DTTM   <- convert_dateViol(df_bldgData$Status_DTTM)

# Combine building data for "all-of-City" with fire incident data, keeping only cases where
# the fire incident post-dated the code violation(s)
tmp1 <- df_bldgData    %>% group_by(ST_NUM, ST_NAME, ST_NAME_SUF)
tmp2 <- df_fireRelated %>% group_by(ST_NUM, ST_NAME, ST_NAME_SUF)
tmp  <- left_join(tmp1, tmp2, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
idx  <- which(tmp$Alarm.Date <= tmp$Status_DTTM)
tmp  <- tmp[-idx,]

df_bldgData <- as.data.frame(tmp)

# Add the outcome variable (all records with an fire Incident.Number are 1, rest are 0)
df_bldgData$outcome <- vector(mode="integer", length = length(df_bldgData$ST_NUM))
idx <- which(!is.na(df_bldgData$Incident.Number))
df_bldgData$outcome[idx] <- 1

if (1) {
  save.image(file = "dataPrepped.RData")
} else {
  load("dataPrepped.RData")
}

df_main       <- df_bldgData
df_main$Value <- as.integer(df_main$Value)

# Features for modeling
features = c("ST_NUM", "ST_NAME", "ST_NAME_SUF", "Code", "PTYPE", "LU", "OWN_OCC", "YR_BUILT", "Value", "AV_BLDG", "outcome")

# Other info we may want later
metadata = c("Status_DTTM", "Latitude", "Longitude", "Description", "Property.Description", "Code",
             "ST_NUM", "ST_NAME", "ST_NAME_SUF", "GROSS_AREA", "AV_BLDG", "PTYPE")

# Subset features and metadata
df_main.features <- df_main[, colnames(df_main) %in% features]
df_main.metadata <- df_main[, colnames(df_main) %in% metadata]

# Create keys to help with grouping
df_main.features$bldgKey <- remove_whitespace(paste(df_main.features$ST_NUM, df_main.features$ST_NAME,
                                                    df_main.features$ST_NAME_SUF,sep=""))
df_main.metadata$bldgKey <- remove_whitespace(paste(df_main.metadata$ST_NUM, df_main.metadata$ST_NAME,
                                                    df_main.metadata$ST_NAME_SUF, sep=""))

# Load and use my mapping of detailed violation codes to broad categories
codeMappings <- read.csv("codes.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)
df_main.features$ViolationGroup <- mapvalues(df_main.features$Code, codeMappings$Code, codeMappings$ViolationGroup)

# Load and use my mapping of LU codes to broad categories
luMappings <- read.csv("lu_codes.csv", na.strings = "", quote = '"', row.names = NULL, stringsAsFactors = FALSE)
df_main.features$LUGroup <- mapvalues(df_main.features$LU, luMappings$LU, luMappings$cat)

# Group all data by address (via bldgKey)
df_base <- df_main.features %>% group_by(bldgKey) %>% summarise(Value=median(Value, na.rm=TRUE),
                                                                LUGROUP=list(LUGroup),
                                                                OWN_OCC=mean(OWN_OCC=="Y"),
                                                                violationCount=length(Code),
                                                                AV_BLDG=mean(AV_BLDG, na.rm=TRUE),
                                                                YR_BUILT=mean(YR_BUILT, na.rm=TRUE),
                                                                rules=sum(ViolationGroup=='rules', na.rm=TRUE),
                                                                maint=sum(ViolationGroup=='maint', na.rm=TRUE),
                                                                trash=sum(ViolationGroup=='trash', na.rm=TRUE),
                                                                safety=sum(ViolationGroup=='safety', na.rm=TRUE),
                                                                vandal=sum(ViolationGroup=='vandal', na.rm=TRUE),
                                                                neg=sum(ViolationGroup=='neg', na.rm=TRUE),
                                                                outcome=max(outcome))

df_info <- df_main.metadata %>% group_by(bldgKey)

# Special handling of LU field (ad hoc)
df_base$LU <- vector(mode="character", length = length(df_base$bldgKey))

for (i in 1:length(df_base$bldgKey)) {
  print(i)
  lu <- unique(df_base[i,]$LUGROUP[[1]])
  if (length(lu) > 1) {
    if (("res" %in% lu) & ("nonres" %in% lu)) {
      df_base[i,]$LU <- "mix"
    }
    if ("mix" %in% lu) {
      df_base[i,]$LU <- "mix"
    }
    if ("other" %in% lu) {
      df_base[i,]$LU <- "other"
    }
    if ("exempt" %in% lu) {
      df_base[i,]$LU <- "exempt"
    }
    if ("condoBldg" %in% lu) {
      df_base[i,]$LU <- "condoBldg"
    }
  } else {
    df_base[i,]$LU <- as.character(unique(df_base[i,]$LUGROUP[[1]]))
  }
}

df_base <- as.data.frame(ungroup(df_base))
df_info <- as.data.frame(ungroup(df_info))

# Drop some things and the address fields from features (only kept them for the above step)
df_base$LUGROUP     <- NULL
df_base$ST_NUM      <- NULL
df_base$ST_NAME     <- NULL
df_base$ST_NAME_SUF <- NULL
df_base$bldgKey     <- NULL

# Label the binary outcomes
df_base$outcome[df_base$outcome==1] <- "fire"
df_base$outcome[df_base$outcome==0] <- "notfire"

# Quick fixes (some hard coded; not ideal, but OK)
df_base$Value[is.na(df_base$Value)]       <- 25
df_base$YR_BUILT[df_base$YR_BUILT<1776]   <- NA
df_base$YR_BUILT[is.na(df_base$YR_BUILT)] <- 1905
df_base$LU[df_base$LU==""] <- "other"

# Make factors
df_base$LU      <- factor(df_base$LU)
df_base$outcome <- factor(df_base$outcome)

# Normalize the numeric features
df_base$Value          <- normalize(as.integer(df_base$Value))
df_base$YR_BUILT       <- normalize(as.integer(df_base$YR_BUILT))
df_base$violationCount <- normalize(as.integer(df_base$violationCount))
df_base$AV_BLDG        <- normalize(as.integer(df_base$AV_BLDG))



############ Modeling ############
trainFraction <- 0.7

# Split data
trainIndex <- as.vector(createDataPartition(df_base$outcome, p = trainFraction, list = FALSE, times = 1))
train_data <- df_base[trainIndex,]
test_data  <- df_base[-trainIndex,]

cvNum     <- 10
repeatNum <- 0

if (repeatNum > 0) {
  fitControl <- trainControl(method = "repeatedcv", number = cvNum, repeats = repeatNum,
                             allowParallel = TRUE, verboseIter = TRUE, classProbs = TRUE)
} else {
  fitControl <- trainControl(method = "cv", number = cvNum,
                             allowParallel = TRUE, verboseIter = TRUE, classProbs = TRUE)  
}

# modelType = "gbm"
gbm.tuneGrid = expand.grid(interaction.depth = c(4, 8),
                           n.trees = c(100),
                           shrinkage = c(0.01, 0.05),
                           n.minobsinnode = c(4, 6))

# modelType = "rf"
rf.tuneGrid <- expand.grid(mtry = c(4,6,8))
rf.ntree    <- 100

# modelType = "nnet"
nnet.tuneGrid <- expand.grid(.decay = c(0.05, 0.01), .size = c(12, 16))

# modelType = "svm"
svm.tuneGrid <- expand.grid(.sigma=c(0, 0.05, 0.1), .c = c(4, 8, 16))

modelType <- "rf"

foo <- colnames(train_data)
idx <- which(foo=="outcome")

if (modelType == "rf") {
  rfFit <- train(x = train_data[, -idx], y = train_data$outcome,
                 method = "rf",
                 trControl = fitControl,
                 verbose = TRUE,
                 allowParallel = TRUE,
                 ntree = rf.ntree,
                 tuneGrid = rf.tuneGrid,
                 importance = TRUE,
                 replace = FALSE)
  mdl <- rfFit
}

if (modelType == "net") {
  nnetFit <- train(x = train_data[, -idx], y = train_data$outcome,
                   method = "nnet",
                   tuneGrid = nnet.tuneGrid,
                   maxit = 1000,
                   verbose = TRUE)
  mdl <- nnetFit
}

if (modelType == "svm") {
  svmFit <- train(x = train_data[, -idx], y = train_data$outcome,
                  method = "svmLinear",
                  tuneGrid = svm.tuneGrid,
                  maxit = 100,
                  trControl = fitControl,
                  verbose = TRUE)
  mdl <- svmFit
}

if (modelType == "gbm") {
  gbmFit <- train(x = train_data[, -idx], y = train_data$outcome,
                  method = "gbm",
                  trControl = fitControl,
                  verbose = TRUE,
                  allowParallel = TRUE,
                  tuneGrid = gbm.tuneGrid)
  
  mdl = gbmFit
}

stopCluster(cl)

fitted.results <- predict(mdl, newdata=test_data, type='raw')
confusionMatrix(table(fitted.results, test_data$outcome))

fitted.results <- predict(mdl, newdata=test_data, type='prob')
pred <- as.factor(ifelse(fitted.results$fire > 0.3,'fire','notfire'))
confusionMatrix(table(pred, test_data$outcome))

probs <- predict(mdl, newdata=test_data, type='prob')

if (0) {
  model <- glm(outcome ~., family=binomial(link='logit'), data=train_data)
  fitted.results <- predict(model, newdata=test_data, type='response')
  pred <- as.factor(ifelse(fitted.results > 0.5,'fire','notfire'))
  
  acc <- sum(pred==test_data$outcome)/length(test_data$outcome)*100
  confusionMatrix(table(pred, test_data$outcome))
  
}


# Stuff for map
# mapData <- data.frame(probs, test_data$Value, test_data$violationCount,
#                       test_data$OWN_OCC, test_data$YR_BUILT, test_data$violationCount)
# 
# colnames(mapData) <- c('prob_fire', 'prob_not_fire', 'fee',
#                        'violation_count', 'owner_occ', 'year', 'violationGroup')
# 
# write.csv(mapData, row.names = FALSE, file="map/mapdata.csv")
