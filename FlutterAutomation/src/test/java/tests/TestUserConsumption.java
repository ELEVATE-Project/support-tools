package tests;

import base.BaseTest;

import org.testng.annotations.Test;

import data.TestData;
import flows.LoginFlow;
import flows.ObservationWORFlow;
import flows.ObservationWRFlow;
import flows.ProjectFlow;
import flows.SurveyFlow;

public class TestUserConsumption extends BaseTest {

    @Test
    public void ProjectConsumption() throws Exception {

        System.out.println("===== APP LAUNCHED =====");

        Thread.sleep(10000);

        // Login Flow
        LoginFlow loginFlow = new LoginFlow(driver);

        loginFlow.login(
                TestData.EMAIL,
                TestData.PASSWORD
        );

        System.out.println("===== LOGIN SUCCESSFUL =====");

        Thread.sleep(20000);

        // Open Program card
        ProjectFlow projectFlow = new ProjectFlow(driver);
        projectFlow.openFirstProgram(
            TestData.PROGRAM_NAME,
            TestData.PROJECT_NAME
        );

        // Open Observation Without Rubrics    
        ObservationWORFlow observationWORFlow = new ObservationWORFlow(driver);
        observationWORFlow.startConsumptionWORObservation(
                TestData.PROGRAM_NAME,
                TestData.ObservationWOR_Name
        );

        // Open Observation WR
        ObservationWRFlow observationWRFlow = new ObservationWRFlow(driver);
        observationWRFlow.startConsumptionWRbservation(
                TestData.PROGRAM_NAME,
                TestData.ObservationWR_Name
        );

        // Open Survey      
        SurveyFlow surveyFlow = new SurveyFlow(driver);
        surveyFlow.startConsumptionSurvey(
                TestData.PROGRAM_NAME,
                TestData.SurveyName
        );
    }
}