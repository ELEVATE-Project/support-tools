package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
import pages.ProjectPage;

public class ProjectFlow extends BaseFlow {

    private ProjectPage projectPage;

    public ProjectFlow(AndroidDriver driver) {

        super(driver);
        projectPage = new ProjectPage(driver);
    }

    public void openFirstProgram(String programName, String projectName) throws Exception {

        log("OPENING PROGRAM");
        printPageSource();

        projectPage.clickProgramCard();

        log("SEARCHING PROGRAM");
        projectPage.searchProgram(programName);
        Thread.sleep(3000);
        projectPage.clickProgram(programName);

        projectPage.clickProgramCardByName();

        projectPage.clickProject(projectName);

        log("STARTING PROJECT");

        projectPage.startProject();
        Thread.sleep(3000);
        projectPage.startProject();
        Thread.sleep(3000);
        log("PROJECT STARTED");
        printPageSource();
        Thread.sleep(2000);
        projectPage.clickTaskDetails();

        /* ---------------- TASK 1 ---------------- */

        log("TASK 1");
        Thread.sleep(2000);
        projectPage.clickTask1();

        projectPage.consumeTask1();

        projectPage.clickUploadEvidence();

        projectPage.checkoboxuploadEvidence();

        projectPage.selectFileToUpload();

        projectPage.clickImageToUpload();

        projectPage.clickToAttachFile();

        driver.navigate().back();

        /* ---------------- TASK 2 ---------------- */

        log("TASK 2");

        projectPage.clickTask2();

        projectPage.consumeTask2();

        driver.navigate().back();

        /* ---------------- TASK 3 ---------------- */

        log("TASK 3");

        projectPage.clickTask3();

        projectPage.consumeTask3();

        driver.navigate().back();

        /* ---------------- TASK 4 ---------------- */

        log("TASK 4");

        projectPage.clickTask4();

        projectPage.consumeTask4();

        driver.navigate().back();

        /* ---------------- TASK 5 ---------------- */

        log("TASK 5");

        projectPage.loadMoreButton();

        projectPage.clickTask5();

        projectPage.consumeTask5();

        driver.navigate().back();

        /* ---------------- SUBMIT ---------------- */

        log("SUBMITTING IMPROVEMENT");

        projectPage.clickSubmitImprovement();

        projectPage.selectFileToUpload();

        printPageSource();

        log("ADDING PROJECT LEVEL EVIDENCE");

        projectPage.projectCheckBoxUploadEvidence();

        projectPage.clickImageToUpload();

        log("SUBMITTING PROJECT");
        
        projectPage.clickSubmitImprovement();

        projectPage.confirmSubmit();

        /* ---------------- CERTIFICATE ---------------- */

        log("VERIFYING CERTIFICATE");
        Thread.sleep(20000);
        projectPage.clickCertificateCard();

        log("PLEASE CHECK CERTIFICATE MANUALLY");
        // printPageSource();

        // if (projectPage.isCertificateDisplayed()) {
        //     log("CERTIFICATE GENERATED SUCCESSFULLY");
        // } else {
        //     log("CERTIFICATE NOT GENERATED");
        // }
        Thread.sleep(10000);
        driver.navigate().back();
        Thread.sleep(5000);
        driver.navigate().back();


    }
}