package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
// import pages.ProjectPage;
import pages.SurveyPage;

public class SurveyFlow extends BaseFlow {

    private SurveyPage surveyPage;
    // private ProjectPage projectPage;

    public SurveyFlow(AndroidDriver driver) {

        super(driver);

        // projectPage = new ProjectPage(driver);
        surveyPage = new SurveyPage(driver);
    }

    public void startConsumptionSurvey(String programName, String surveyName) throws Exception {

        // log("OPENING PROGRAM");
        // printPageSource();

        // projectPage.clickProgramCard();

        // log("PROGRAM OPENED");
        // printPageSource();

        // log("SEARCHING PROGRAM");
        // projectPage.searchProgram(programName);
        // log("PROGRAM SEARCHED");
        // projectPage.clickProgram(programName);

        log("OPENING SURVEY");
        surveyPage.clickSurvey();

        surveyPage.clickSurveyByName(surveyName);

        log("SURVEY OPENED");
        printPageSource();

        log("ANSWERING QUESTION 1");
        surveyPage.consumeSurveyQ1();

        log("ANSWERING QUESTION 2");
        surveyPage.consumeSurveyQ2();

        log("OPENING PAGE 2");
        surveyPage.clickPage2();

        log("ANSWERING QUESTION 3");
        surveyPage.consumeSurveyQ3();

        log("OPENING PAGE 3");
        surveyPage.clickPage3();

        log("ANSWERING QUESTION 4");
        surveyPage.consumeSurveyQ3P3();

        log("SUBMITTING SURVEY");
        surveyPage.submitSurvey();

        log("SURVEY SUBMITTED");

        driver.navigate().back();
    }
}