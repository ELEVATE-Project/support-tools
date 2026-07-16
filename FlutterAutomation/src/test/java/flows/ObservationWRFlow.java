package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
import pages.ObservationWRPage;
// import pages.ProjectPage;

public class ObservationWRFlow extends BaseFlow {

    private ObservationWRPage observationWRPage;
    // private ProjectPage projectPage;

    public ObservationWRFlow(AndroidDriver driver) {

        super(driver);

        // projectPage = new ProjectPage(driver);
        observationWRPage = new ObservationWRPage(driver);
    }

    public void startConsumptionWRbservation(String programName, String observationWRName) throws Exception {

        /* ---------------- OPEN PROGRAM ---------------- */

        // log("OPENING PROGRAM");
        // printPageSource();

        // projectPage.clickProgramCard();

        // log("SEARCHING PROGRAM");

        // projectPage.searchProgram(programName);

        // projectPage.clickProgram(programName);

        // projectPage.clickProgramCardByName();

        /* ---------------- OPEN OBSERVATION ---------------- */

        log("OPENING OBSERVATION");
        Thread.sleep(2000);
        observationWRPage.clickObs();

        observationWRPage.clickObsWOR(observationWRName);

        observationWRPage.clickAddSchool();

        observationWRPage.selectSchool();

        observationWRPage.clickStartObservation();

        /* ---------------- OBSERVATION 1 ---------------- */

        log("OPENING OBSERVATION 1");

        observationWRPage.clickObservation1();


        /* ---------------- DOMAIN 1 ---------------- */

        observationWRPage.select1stDomain();

        log("DOMAIN 1");

        observationWRPage.consume1stDomain1Question();

        observationWRPage.consume1stDomain2Question();

        Thread.sleep(10000);
        // observationWRPage.consume1stDomain3Question();

        observationWRPage.consume1stDomain4Question();

        observationWRPage.submit1stDomainResponse();

        observationWRPage.confirmSubmit1stDomainResponse();

        driver.navigate().back();

        /* ---------------- DOMAIN 2 ---------------- */

        log("DOMAIN 2");

        observationWRPage.click2ndDomain();

        observationWRPage.consume2ndDomain1Question();

        observationWRPage.consume2ndDomain2Question();

        observationWRPage.consume2ndDomain3Question();

        observationWRPage.submit2ndDomain4Response();

        observationWRPage.selectnextpage();

        observationWRPage.submit2ndDomain5Response();

        observationWRPage.submit2ndDomainResponse();

        observationWRPage.confirmSubmit2ndDomainResponse();

        driver.navigate().back();

        /* ---------------- DOMAIN 3 ---------------- */

        log("DOMAIN 3");

        observationWRPage.click3rdDomain();

        observationWRPage.consume3rdDomain1Question();

        observationWRPage.submit3rdDomainResponse();

        observationWRPage.confirmSubmit3rdDomainResponse();

        driver.navigate().back();

        /* ---------------- DOMAIN 4 ---------------- */

        log("DOMAIN 4");

        observationWRPage.click4thDomain();

        observationWRPage.consume4thDomain1Question();

        observationWRPage.consume4thDomain2Question();

        observationWRPage.submit4thDomainResponse();

        observationWRPage.confirmSubmit4thDomainResponse();

        /* ---------------- COMPLETE OBSERVATION ---------------- */

        log("COMPLETING OBSERVATION");

            // observationWRPage.selectcompletedObservationWOR();

            // observationWRPage.confirmsubmit();

        driver.navigate().back();
        Thread.sleep(2000);

        driver.navigate().back();

        // Thread.sleep(2000);

        // driver.navigate().back();

        Thread.sleep(5000);
        /* ---------------- REPORT ---------------- */

        log("OPENING REPORT");

        observationWRPage.seecompletedObservationWR();

        observationWRPage.clickReports();

        log("OBSERVATION WR COMPLETED SUCCESSFULLY");

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