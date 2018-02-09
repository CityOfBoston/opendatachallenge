# Modeling Boston Fire Risk
# Updated: May 7, 2017

library(plyr)
library(dplyr)
library(stringr)
library(caret)
library(xgboost)
library(doParallel)
library(parallel)
library(foreach)
library(pROC)

rm(list = ls())

doParallel = TRUE

if (doParallel) {
  nCores = detectCores() - 2
  cl <- makeCluster(nCores)
  registerDoParallel(cl)
  opts <- list(preschedule=TRUE)
}

######## Generic Functions #########

remove_whitespace           <- function(x) {return(gsub("\\s", "", x))}
remove_nonNumeric           <- function(x) {return(gsub("[[:alpha:]]*", "", x))}
remove_hyphen               <- function(x) {return(gsub("-", "", x))}
convert_dateAlarm           <- function(x) {return(as.Date(x, "%m/%d/%y"))}
convert_dateViol            <- function(x) {return(as.Date(x, "%m/%d/%Y %H:%M"))}
remove_punct                <- function(x) {return(gsub("[[:punct:]]*", "", x))}

# Function to split street numbers
split_fun <- function(x) {
  tmp <- strsplit(x, " +")
  if (length(tmp[[1]]) > 1) {
    return(1)
  } else {
    return(0)
  }
}

# Function to check numeric gap between street numbers
check_st_nums <- function(x) {
  foo <- as.integer(x[2]) - as.integer(x[1])
  if ((is.finite(foo)) & (foo > 2)) {
    return(1)
  } else {
    return(0)
  }
}

# Function to normalize vector between 0 and 1
normalize <- function(x) {
  (x - min(x))/(max(x) - min(x))
}

# Load BFD data and cat it all together
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
df_code$City   <- tolower(remove_punct(df_code$City))
df_code        <- df_code[-which(is.na(df_code$City)),]

# Fill in street number ranges
# Done once, in theory, for code violation data
if (1) {
  idx <- which(!is.na(df_code$StHigh))
  
  df_code_add <- foreach(i = idx, 
          .combine = 'rbind') %dopar% {
            
    a <- df_code$StNo[i]
    b <- df_code$StHigh[i]
    if (a > b) {
      c <- a
      a <- b
      b <- c
    }
    N <- (b-2) - (a+2) + 1
    tmp_code <- NULL
    if (N > 0) {
      digits   <- seq((a+2), (b-2), 2)
      reps     <- length(digits)
      tmp_code <- df_code[rep(seq_len(nrow(df_code[i,])), each=reps),]
      tmp_code$StNo[1:reps] <- digits
    }
    tmp_code
  }
  df_code_aug <- rbind(df_code, df_code_add) # Add these fill-ins to the base code data
  df_code_aug$StNo <- as.character(df_code_aug$StNo)
  save(df_code_aug, file="AugmentedCodeDataSet.RData")
} else {
  load("AugmentedCodeDataSet.RData")
}

# Features that were thought to be relevant - in the end, not all used (they get dropped)
assessFeatures = list("ST_NUM", "ST_NAME", "ST_NAME_SUF", "PTYPE", "LU", "OWN_OCC", "AV_BLDG", "YR_BUILT",
                      "GROSS_AREA", "LIVING_AREA", "AV_BLDG", "R_KITCH")

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
  
  # Combine the data sets together
  foo1      <- anti_join(df_bldgs16, df_bldgs17, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  foo2      <- anti_join(df_bldgs17, df_bldgs16, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  foo3      <- rbind(foo1, foo2)
  foo4      <- foo3[!duplicated(foo3), ]
  df_bldgs  <- rbind(df_bldgs17, foo4)
  foo1      <- anti_join(df_bldgs15, df_bldgs, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  foo2      <- anti_join(df_bldgs, df_bldgs15, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  foo3      <- rbind(foo1, foo2)
  foo4      <- foo3[!duplicated(foo3), ]
  df_bldgs  <- rbind(df_bldgs, foo4)
  foo1      <- anti_join(df_bldgs14, df_bldgs, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  foo2      <- anti_join(df_bldgs, df_bldgs14, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
  foo3      <- rbind(foo1, foo2)
  foo4      <- foo3[!duplicated(foo3), ]
  df_bldgs  <- rbind(df_bldgs, foo4)
  
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
  
  idx <- which((as.vector(sapply(df_bldgs$ST_NUM, split_fun))>0))
  foo <- strsplit(df_bldgs$ST_NUM[idx], " +")
  idx <- which((as.vector(sapply(foo, check_st_nums))>0))
  
  df_bldgs_add <- foreach(i = idx, 
                          .combine = 'rbind') %dopar% {
                           
    foo <- strsplit(df_bldgs$ST_NUM[i], " +")
    a <- as.integer(foo[[1]][1])
    b <- as.integer(foo[[1]][2])
    tmp_bldg <- NULL
    if (is.finite(a) & is.finite(b)) {
      if (a > b) {
        c <- a
        a <- b
        b <- c
      }
      N <- (b-2) - (a+2) + 1
      if (N > 0) {
        digits <- seq((a+2), (b-2), 2)
        reps <- length(digits)
        tmp_bldg <- df_bldgs[rep(seq_len(nrow(df_bldgs[idx[i],])), each=reps),]
        tmp_bldg$ST_NUM[1:reps] <- digits
      }
    }
    tmp_bldg
  }
  df_bldgs_aug <- rbind(df_bldgs, df_bldgs_add) # Add these fill-ins to the base bldg data
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
df_bldgData <- right_join(df_code_aug, df_bldgs_aug, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))

# Records with no violation
idx <- which(is.na(df_bldgData$Ticket_No))
df_bldgData$Ticket_No[idx]   <- "NONE"
df_bldgData$Status_DTTM[idx] <- "01/01/1000 00:00"
df_bldgData$Status[idx]      <- "DNE"
df_bldgData$Value[idx]       <- "0"

# Make dates standard
df_fireRelated$Alarm.Date <- convert_dateAlarm(df_fireRelated$Alarm.Date)
df_bldgData$Status_DTTM   <- convert_dateViol(df_bldgData$Status_DTTM)

# Combine building data for "all-of-City" with fire incident data, keeping only cases where
# the fire incident post-dates the code violation(s)
tmp1 <- df_bldgData    %>% group_by(ST_NUM, ST_NAME, ST_NAME_SUF)
tmp2 <- df_fireRelated %>% group_by(ST_NUM, ST_NAME, ST_NAME_SUF)
tmp  <- left_join(tmp1, tmp2, by=c("ST_NUM", "ST_NAME", "ST_NAME_SUF"))
idx  <- which(tmp$Alarm.Date <= tmp$Status_DTTM)
tmp  <- tmp[-idx,]

df_bldgData <- tmp

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
features = c("ST_NUM", "ST_NAME", "ST_NAME_SUF", "Code", "PTYPE", "LU", "OWN_OCC", "YR_BUILT", "Value", "City",
             "AV_BLDG", "GROSS_AREA", "LIVING_AREA", "R_KITCH", "outcome")

# Other info we may want later
metadata = c("Status_DTTM", "Latitude", "Longitude", "ST_NUM", "ST_NAME", "ST_NAME_SUF")

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
                                                                City=unique(City)[1],
                                                                OWN_OCC=mean(OWN_OCC=="Y"),
                                                                violationCount=length(Code),
                                                                AV_BLDG=max(AV_BLDG, na.rm=TRUE),
                                                                Kitchen=max(R_KITCH, na.rm=TRUE),
                                                                YR_BUILT=mean(YR_BUILT, na.rm=TRUE),
                                                                rules=sum(ViolationGroup=='rules', na.rm=TRUE),
                                                                maint=sum(ViolationGroup=='maint', na.rm=TRUE),
                                                                trash=sum(ViolationGroup=='trash', na.rm=TRUE),
                                                                safety=sum(ViolationGroup=='safety', na.rm=TRUE),
                                                                vandal=sum(ViolationGroup=='vandal', na.rm=TRUE),
                                                                area=mean((GROSS_AREA+LIVING_AREA)/2, na.rm=TRUE),
                                                                neg=sum(ViolationGroup=='neg', na.rm=TRUE),
                                                                outcome=max(outcome))

df_info <- as.data.frame(df_main.metadata) %>% group_by(bldgKey) %>% summarise(ST_NUM=unique(ST_NUM)[1],
                                                                ST_NAME=unique(ST_NAME)[1],
                                                                ST_NAME_SUF=unique(ST_NAME_SUF)[1],
                                                                Latitude=mean(Latitude, na.rm=TRUE),
                                                                Longitude=mean(Longitude, na.rm=TRUE))

# Special handling of LU field (ad hoc)
df_base$LU <- vector(mode="character", length = length(df_base$bldgKey))

df_base_LU <- foreach(i = 1:length(df_base$bldgKey), 
                      .combine = 'rbind') %dopar% {

  foo <- df_base[i,]
  lu  <- unique(foo$LUGROUP[[1]])
  if (length(lu) > 1) {
    if ("other" %in% lu) {
      foo$LU <- "other"
    }
    if ("mix" %in% lu) {
      foo$LU <- "mix"
    }
    if ("exempt" %in% lu) {
      foo$LU <- "exempt"
    }
    if (("res" %in% lu) & ("nonres" %in% lu)) {
      foo$LU <- "mix"
    }
    if ("res" %in% lu) {
      foo$LU <- "res"
    }
    if ("condoBldg" %in% lu) {
      foo$LU <- "condoBldg"
    }
  }
  foo
}

df_base <- as.data.frame(ungroup(df_base_LU))
df_info <- as.data.frame(ungroup(df_info))

# Drop some fields no longer needed
df_base$LUGROUP <- NULL
df_base$bldgKey <- NULL

# Label the binary outcomes
df_base$outcome[df_base$outcome==1] <- "fire"
df_base$outcome[df_base$outcome==0] <- "notfire"

# Quick fixes (fill with medians; not ideal, but will have to suffice)
df_base$Value[is.na(df_base$Value)]       <- 25
df_base$YR_BUILT[df_base$YR_BUILT<1776]   <- NA
df_base$YR_BUILT[is.na(df_base$YR_BUILT)] <- 1905
df_base$LU[df_base$LU==""]                <- "other"
df_base$area[df_base$area<50]             <- NA
df_base$area[is.na(df_base$area)]         <- 3092
df_base$City[df_base$City==""]            <- "unknown"
df_base$City[is.na(df_base$City)]         <- "unknown"
df_base$Kitchen[is.na(df_base$Kitchen)]   <- 0

# Make factors
df_base$LU      <- factor(df_base$LU)
df_base$outcome <- factor(df_base$outcome)
df_base$City    <- factor(df_base$City)

# Normalize the numeric features
df_base$Value          <- normalize(as.integer(df_base$Value))
df_base$YR_BUILT       <- normalize(as.integer(df_base$YR_BUILT))
df_base$violationCount <- normalize(as.integer(df_base$violationCount))
df_base$AV_BLDG        <- normalize(as.integer(df_base$AV_BLDG))
df_base$area           <- normalize(as.integer(df_base$area))
df_base$rules          <- normalize(as.integer(df_base$rules))
df_base$maint          <- normalize(as.integer(df_base$maint))
df_base$neg            <- normalize(as.integer(df_base$neg))
df_base$trash          <- normalize(as.integer(df_base$trash))
df_base$safety         <- normalize(as.integer(df_base$safety))
df_base$vandal         <- normalize(as.integer(df_base$vandal))
df_base$Kitchen        <- normalize(as.integer(df_base$Kitchen))



############ Modeling ############
trainFraction <- 0.7

# Split data
trainIndex <- as.vector(createDataPartition(df_base$outcome, p = trainFraction, list = FALSE, times = 1))
train_data <- df_base[trainIndex,]
test_data  <- df_base[-trainIndex,]

train_meta <- df_info[trainIndex,]
test_meta  <- df_info[-trainIndex,]

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
rf.tuneGrid <- expand.grid(mtry = c(2,3,4))
rf.ntree    <- 200

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
                 replace = TRUE)
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
                  n.trees=1000)
  
  mdl = gbmFit
}

stopCluster(cl)

fitted.results <- predict(mdl, newdata=test_data[,-idx])
confusionMatrix(table(fitted.results, test_data$outcome))

fitted.results <- predict(mdl, newdata=test_data[,-idx], type='prob')

tru  <- ifelse(test_data$outcome =='fire',1,0)
pred <- fitted.results[,1]
roc(tru, pred,plot=TRUE)

# Dial in 50% FA
pred <- as.factor(ifelse(fitted.results$fire > 0.33,'fire','notfire'))
confusionMatrix(table(pred, test_data$outcome))

probs <- predict(mdl, newdata=test_data[,-idx], type='prob')
mapData <- cbind(test_data, test_meta, probs)

write.csv(mapData, row.names = FALSE, file="map/mapdata.csv")