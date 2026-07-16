package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
import pages.ObservationWORPage;

public class ObservationWORFlow extends BaseFlow {

    private ObservationWORPage observationWORPage;

    public ObservationWORFlow(AndroidDriver driver) {

        super(driver);

        observationWORPage = new ObservationWORPage(driver);
    }

    public void startConsumptionWORObservation(String programName, String observationWORName) throws Exception {

        /* ---------------- OPEN OBSERVATION ---------------- */

        log("OPENING OBSERVATION");

        observationWORPage.clickObs();

        observationWORPage.clickObsWOR(observationWORName);

        observationWORPage.clickAddSchool();

        observationWORPage.selectSchool();

        Thread.sleep(2000);
        observationWORPage.clickStartObservation();

        // /* ---------------- OBSERVATION ---------------- */

        log("OPENING OBSERVATION");

        observationWORPage.clickObservation1();

        /* ---------------- PAGE 1 ---------------- */

        log("QUESTION 1");
        observationWORPage.consumeFirstQuestion();

        printPageSource();

        Thread.sleep(10000);
        // observationWORPage.consume2Question();

        log("QUESTION 3");
        observationWORPage.consume3Question();

        log("QUESTION 5");
        observationWORPage.consume5Question();

        log("QUESTION 6");
        observationWORPage.consume6Question();

        log("QUESTION 7");
        observationWORPage.consume7Question();

        log("QUESTION 9");
        observationWORPage.consume9Question();

        /* ---------------- PAGE 2 ---------------- */

        log("OPENING PAGE 2");

        observationWORPage.moveToPage2();

        log("QUESTION 10");

        observationWORPage.consume10Question();

        log("SUBMITTING OBSERVATION");

        observationWORPage.clickSubmitTask10();

        observationWORPage.confirmsubmit();

        driver.navigate().back();

        /* ---------------- REPORT ---------------- */

        log("OPENING REPORT");
        Thread.sleep(2000);
        printPageSource();

        observationWORPage.seecompletedObservationWOR();

        observationWORPage.clickReports();

        log("OBSERVATION WOR COMPLETED SUCCESSFULLY");

        printPageSource();

        Thread.sleep(5000);

        driver.navigate().back();

        Thread.sleep(2000);
    
        driver.navigate().back();

        Thread.sleep(2000);

        driver.navigate().back();

        Thread.sleep(5000);


    }
}