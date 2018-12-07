#' Comparative Cohort Analysis
#'
#' @param treatment The treatment cohort definition identifier
#' @param comparator The comparator cohort definition identifier
#' @param outcome The outcome cohort definition identifier
#' @keywords comparative cohort
#' @import DatabaseConnector
#' @import CohortMethod
#' @import FeatureExtraction
#' @export
#'

executeComparativeCohortAnalysis <-
  function(treatmentId,
           comparatorId,
           outcomeId,
           modelType,
           executionId,
           dbms,
           connectionString,
           cdmTableQualifier,
           resultsTableQualifier,
           timeAtRiskStart,
           timeAtRiskEnd,
           addExposureDaysToEnd,
           minimumWashoutPeriod,
           minimumDaysAtRisk,
           rmSubjectsInBothCohorts,
           rmPriorOutcomes,
           psAdjustment,
           psExclusions = c(),
           psInclusions = c(),
           psDemographics,
           psDemographicsGender,
           psDemographicsRace,
           psDemographicsEthnicity,
           psDemographicsAge,
           psDemographicsYear,
           psDemographicsMonth,
           psTrim,
           psTrimFraction,
           psMatch,
           psMatchMaxRatio,
           psStrat,
           psStratNumStrata,
           psConditionOcc,
           psConditionOcc365d,
           psConditionOcc30d,
           psConditionOccInpt180d,
           psConditionEra,
           psConditionEraEver,
           psConditionEraOverlap,
           psConditionGroup,
           psConditionGroupMeddra,
           psConditionGroupSnomed,
           psDrugExposure,
           psDrugExposure365d,
           psDrugExposure30d,
           psDrugEra,
           psDrugEra365d,
           psDrugEra30d,
           psDrugEraOverlap,
           psDrugEraEver,
           psDrugGroup,
           psProcedureOcc,
           psProcedureOcc365d,
           psProcedureOcc30d,
           psProcedureGroup,
           psObservation,
           psObservation365d,
           psObservation30d,
           psObservationCount365d,
           psMeasurement,
           psMeasurement365d,
           psMeasurement30d,
           psMeasurementCount365d,
           psMeasurementBelow,
           psMeasurementAbove,
           psConceptCounts,
           psRiskScores,
           psRiskScoresCharlson,
           psRiskScoresDcsi,
           psRiskScoresChads2,
           psRiskScoresChads2vasc,
           psInteractionYear,
           psInteractionMonth,
           omCovariates,
           omExclusions = c(),
           omInclusions = c(),
           omDemographics,
           omDemographicsGender,
           omDemographicsRace,
           omDemographicsEthnicity,
           omDemographicsAge,
           omDemographicsYear,
           omDemographicsMonth,
           omTrim,
           omTrimFraction,
           omMatch,
           omMatchMaxRatio,
           omStrat,
           omStratNumStrata,
           omConditionOcc,
           omConditionOcc365d,
           omConditionOcc30d,
           omConditionOccInpt180d,
           omConditionEra,
           omConditionEraEver,
           omConditionEraOverlap,
           omConditionGroup,
           omConditionGroupMeddra,
           omConditionGroupSnomed,
           omDrugExposure,
           omDrugExposure365d,
           omDrugExposure30d,
           omDrugEra,
           omDrugEra365d,
           omDrugEra30d,
           omDrugEraOverlap,
           omDrugEraEver,
           omDrugGroup,
           omProcedureOcc,
           omProcedureOcc365d,
           omProcedureOcc30d,
           omProcedureGroup,
           omObservation,
           omObservation365d,
           omObservation30d,
           omObservationCount365d,
           omMeasurement,
           omMeasurement365d,
           omMeasurement30d,
           omMeasurementCount365d,
           omMeasurementBelow,
           omMeasurementAbove,
           omConceptCounts,
           omRiskScores,
           omRiskScoresCharlson,
           omRiskScoresDcsi,
           omRiskScoresChads2,
           omRiskScoresChads2vasc,
           omInteractionYear,
           omInteractionMonth,
           delCovariatesSmallCount,
           negativeControls) {
    executionOutput <-
      file(paste("execution_",executionId,".txt",sep = ""))
    sink(executionOutput, append = TRUE)
    sink(executionOutput, append = TRUE, type = "message")

    # disable scientific notation, necessary for our grouping on ps later
    options(scipen = 999)

    # log environment settings
    as.list(environment(), all=TRUE)

    connectionDetails <-
      createConnectionDetails(dbms = dbms, connectionString = connectionString)

    # obtain the connection
    connection <- connect(connectionDetails)

    covariateSettings <- createCovariateSettings(
      useCovariateDemographics = psDemographics,
      useCovariateDemographicsGender = psDemographicsGender,
      useCovariateDemographicsRace = psDemographicsRace,
      useCovariateDemographicsEthnicity = psDemographicsEthnicity,
      useCovariateDemographicsAge = psDemographicsAge,
      useCovariateDemographicsYear = psDemographicsYear,
      useCovariateDemographicsMonth = psDemographicsMonth,
      useCovariateConditionOccurrence = psConditionOcc,
      useCovariateConditionOccurrence365d = psConditionOcc365d,
      useCovariateConditionOccurrence30d = psConditionOcc30d,
      useCovariateConditionOccurrenceInpt180d = psConditionOccInpt180d,
      useCovariateConditionEra = psConditionEra,
      useCovariateConditionEraEver = psConditionEraEver,
      useCovariateConditionEraOverlap = psConditionEraOverlap,
      useCovariateConditionGroup = psConditionGroup,
      useCovariateConditionGroupMeddra = psConditionGroupMeddra,
      useCovariateConditionGroupSnomed = psConditionGroupSnomed,
      useCovariateDrugExposure = psDrugExposure,
      useCovariateDrugExposure365d = psDrugExposure365d,
      useCovariateDrugExposure30d = psDrugExposure30d,
      useCovariateDrugEra = psDrugEra,
      useCovariateDrugEra365d = psDrugEra365d,
      useCovariateDrugEra30d = psDrugEra30d,
      useCovariateDrugEraOverlap = psDrugEraOverlap,
      useCovariateDrugEraEver = psDrugEraEver,
      useCovariateDrugGroup = psDrugGroup,
      useCovariateProcedureOccurrence = psProcedureOcc,
      useCovariateProcedureOccurrence365d = psProcedureOcc365d,
      useCovariateProcedureOccurrence30d = psProcedureOcc30d,
      useCovariateProcedureGroup = psProcedureGroup,
      useCovariateObservation = psObservation,
      useCovariateObservation365d = psObservation365d,
      useCovariateObservation30d = psObservation30d,
      useCovariateObservationCount365d = psObservationCount365d,
      useCovariateMeasurement = psMeasurement,
      useCovariateMeasurement365d = psMeasurement365d,
      useCovariateMeasurement30d = psMeasurement30d,
      useCovariateMeasurementCount365d = psMeasurementCount365d,
      useCovariateMeasurementBelow = psMeasurementBelow,
      useCovariateMeasurementAbove = psMeasurementAbove,
      useCovariateConceptCounts = psConceptCounts,
      useCovariateRiskScores = psRiskScores,
      useCovariateRiskScoresCharlson = psRiskScoresCharlson,
      useCovariateRiskScoresDCSI = psRiskScoresDcsi,
      useCovariateRiskScoresCHADS2 = psRiskScoresChads2,
      useCovariateRiskScoresCHADS2VASc = psRiskScoresChads2vasc,
      useCovariateInteractionYear = psInteractionYear,
      useCovariateInteractionMonth = psInteractionMonth,
      excludedCovariateConceptIds = psExclusions,
      includedCovariateConceptIds = psInclusions,
      deleteCovariatesSmallCount = delCovariatesSmallCount
    )

    # todo: configure negative controls

    cmd <- getDbCohortMethodData(
      connectionDetails,
      cdmDatabaseSchema = cdmTableQualifier,
      exposureDatabaseSchema = resultsTableQualifier,
      outcomeDatabaseSchema = resultsTableQualifier,
      cdmVersion = 5,
      exposureTable = "COHORT",
      outcomeTable = "COHORT",
      targetId = treatmentId,
      comparatorId = comparatorId,
      outcomeIds = outcomeId,
      washoutPeriod = minimumWashoutPeriod,
      firstExposureOnly = FALSE,
      removeDuplicateSubjects = rmSubjectsInBothCohorts,
      excludeDrugsFromCovariates = FALSE,
      covariateSettings = covariateSettings
    )
	
	if (is.character(cmd))
	{
		outcomeModelPop <- studyPop
		
		warnings <- data.frame(execution_id = executionId,type = 1,warnings = cmd)
		insertTable(
				connection = connection,
				tableName = paste(resultsTableQualifier,"cca_warnings",sep = "."),
				data = warnings,
				dropTableIfExists = FALSE,
				createTable = FALSE
		)
		stop(cmd);
	}

    studyPop <- createStudyPopulation(
      cohortMethodData = cmd,
      outcomeId = outcomeId,
      removeSubjectsWithPriorOutcome = rmPriorOutcomes,
      minDaysAtRisk = minimumDaysAtRisk,
      riskWindowStart = timeAtRiskStart,
      riskWindowEnd = timeAtRiskEnd,
      addExposureDaysToEnd = addExposureDaysToEnd
    )
    
	matchOrStrat <- FALSE
	
    if (psAdjustment) {
      ps <- createPs(
        cohortMethodData = cmd,
        population = studyPop,
        errorOnHighCorrelation = FALSE, # model diagnostics can be returned
        prior = createPrior(
          "laplace",
          exclude = c(0),
          useCrossValidation = TRUE
        ),
        control = createControl(
          maxIterations = 9999,
          cvType = "auto",
          startingVariance = 0.01,
          noiseLevel = "quiet",
          tolerance  = 2e-07,
          cvRepetitions = 10,
          threads = 10
        )
      )
	  
	  if (is.character(ps))
	  {
		  outcomeModelPop <- studyPop
		  
		  warnings <- data.frame(execution_id = executionId,type = 1,warnings = ps)
		  insertTable(
				  connection = connection,
				  tableName = paste(resultsTableQualifier,"cca_warnings",sep = "."),
				  data = warnings,
				  dropTableIfExists = FALSE,
				  createTable = FALSE
		  )
	  }else
	  {
	      auc <- computePsAuc(ps)
	
	      psModel <- getPsModel(propensityScore = ps,
	                            cohortMethodData = cmd)
	
	      strataPop <- ps
	
	      if (psTrimFraction > 0) {
	        psTrimFraction <- psTrimFraction / 100
	      }
	
	      if (psTrim == 1) {
	        strataPop <- trimByPs(strataPop, trimFraction = psTrimFraction)
	      } else if (psTrim == 2) {
	        strataPop <-
	          trimByPsToEquipoise(strataPop, bounds = c(psTrimFraction, 1 - psTrimFraction))
	      }
	
	      if (psMatch == 1) {
	        strataPop <- matchOnPs(
	          strataPop,
	          caliper = 0.25,
	          caliperScale = "standardized",
	          maxRatio = psMatchMaxRatio
	        )
	      } else if (psMatch == 2) {
	        strataPop <-
	          stratifyByPs(strataPop,numberOfStrata = psStratNumStrata)
	      }
	
	      # generate aggregates for propensity score plot
	      workingPs <- strataPop
	      workingPs$propensityScore <-
	        round(workingPs$propensityScore,2)
	      workingPs$preferenceScore <-
	        round(workingPs$preferenceScore,2)
	      aggregates <- aggregate(
	        workingPs$rowId,
	        by = list(workingPs$propensityScore, workingPs$preferenceScore, workingPs$treatment),
	        FUN = length
	      )
	
	      treatment_aggregates <-
	        aggregates[aggregates$Group.3 == 1,c(1,2,4)]
	      comparator_aggregates <-
	        aggregates[aggregates$Group.3 == 0,c(1,2,4)]
	
	      agg_summary <-
	        merge(treatment_aggregates, comparator_aggregates,by = c("Group.1","Group.2"),all = TRUE)
	      agg_summary[is.na(agg_summary)] <- 0
	      colnames(agg_summary) <- c("propensity_score", "preference_score", "treatment", "comparator")
	      agg_summary <-
	        data.frame(execution_id = executionId, agg_summary)
	
	      # save results to database
	      insertTable(
	        connection = connection,
	        tableName = paste(resultsTableQualifier,"cca_psmodel_scores",sep = "."),
	        data = agg_summary,
	        dropTableIfExists = FALSE,
	        createTable = FALSE
	      )
	
	      # save attrition data
	      attrition <- getAttritionTable(strataPop)
	      attrition <-
	        cbind(executionId = executionId, attrition)
	      attrition$attritionOrder <- seq.int(nrow(attrition))
	      colnames(attrition) <-
	        SqlRender::camelCaseToSnakeCase(colnames(attrition))
	
	      insertTable(
	        connection = connection,
	        tableName = paste(resultsTableQualifier,"cca_attrition",sep = "."),
	        data = attrition,
	        dropTableIfExists = FALSE,
	        createTable = FALSE
	      )
	
	      #evaluate covariate balance
	
	      balance <- computeCovariateBalance(strataPop,cmd)
	      colnames(balance)[colnames(balance) == "beforeMatchingsumComparator"] <-
	        "beforeMatchingSumComparator"
	      balance <-
	        cbind(executionId = executionId, balance)
	      colnames(balance) <-
	        SqlRender::camelCaseToSnakeCase(colnames(balance))
	
	      insertTable(
	        connection = connection,
	        tableName = paste(resultsTableQualifier,"cca_balance",sep = "."),
	        data = balance,
	        dropTableIfExists = FALSE,
	        createTable = FALSE
	      )
	
	      # append execution identifier
	      auc <-
	        data.frame(executionId = executionId, auc = auc)
	      psModel <-
	        cbind(executionId = executionId, psModel)
	
	      savePop <-
	        cbind(executionId = executionId, strataPop)
	      savePop$propensityScore <-
	        round(savePop$propensityScore,2)
	      savePop$preferenceScore <-
	        round(savePop$preferenceScore,2)
	
	      # rename columns to snake case
	      colnames(auc) <-
	        SqlRender::camelCaseToSnakeCase(colnames(auc))
	      colnames(psModel) <-
	        SqlRender::camelCaseToSnakeCase(colnames(psModel))
	      colnames(savePop) <-
	        SqlRender::camelCaseToSnakeCase(colnames(savePop))
	
	      # save results to database
	      insertTable(
	        connection = connection,
	        tableName = paste(resultsTableQualifier,"cca_auc",sep = "."),
	        data = auc,
	        dropTableIfExists = FALSE,
	        createTable = FALSE
	      )
	
	      insertTable(
	        connection = connection,
	        tableName = paste(resultsTableQualifier,"cca_psmodel",sep = "."),
	        data = psModel,
	        dropTableIfExists = FALSE,
	        createTable = FALSE
	      )
	
	      insertTable(
	        connection = connection,
	        tableName = paste(resultsTableQualifier,"cca_pop",sep = "."),
	        data = savePop,
	        dropTableIfExists = FALSE,
	        createTable = FALSE
	      )
	      
	      matchOrStrat <- psMatch > 0 | psStrat > 0
	
	      if (matchOrStrat) {
	        outcomeModelPop <- strataPop
	      } else {
	        outcomeModelPop <- studyPop
	      }
	  }
    }else{
    	outcomeModelPop <- studyPop
    }

    
    modelName <- "logistic"

    if (modelType == 1) {
      modelName <- "logistic"
    } else if (modelType == 2) {
      modelName <- "poisson"
    } else if (modelType == 3) {
      modelName <- "cox"
    }

    outcomeModel <- fitOutcomeModel(
      cohortMethodData = cohortMethodData,
      population = outcomeModelPop,
      stratified = matchOrStrat,
      modelType = modelName,
      useCovariates = omCovariates,
      includeCovariateIds = omIncludedConcepts,
      excludeCovariateIds = omExcludedConcepts
    )
	
	if (outcomeModel$outcomeModelStatus != "OK")
	{
		warnings <- data.frame(execution_id = executionId,type = 2,warnings = outcomeModel$outcomeModelStatus)
		insertTable(
				connection = connection,
				tableName = paste(resultsTableQualifier,"cca_warnings",sep = "."),
				data = warnings,
				dropTableIfExists = FALSE,
				createTable = FALSE
		)
	}

    omEstimate <- outcomeModel$outcomeModelTreatmentEstimate
    saveOm <-
      data.frame(
        executionId, treatmentId, comparatorId, outcomeId, exp(omEstimate$logRr), exp(omEstimate$logLb95), exp(omEstimate$logUb95), omEstimate$logRr, omEstimate$seLogRr
      )
    colnames(saveOm) <-
      c(
        "execution_id", "treatment_id", "comparator_id", "outcome_id", "estimate", "lower95", "upper95", "log_rr", "se_log_rr"
      )

    insertTable(
      connection = connection,
      tableName = paste(resultsTableQualifier,"cca_om",sep = "."),
      data = saveOm,
      dropTableIfExists = FALSE,
      createTable = FALSE
    )

    sink()
    sink(type = "message")

    return(as.character(executionId))
  }
