package flows;

import base.BaseFlow;
import io.appium.java_client.android.AndroidDriver;
import pages.HomePage;
import pages.ProfilePage;

public class ProfileFlow extends BaseFlow {

    private HomePage homePage;
    private ProfilePage profilePage;

    public ProfileFlow(AndroidDriver driver) {

        super(driver);

        homePage = new HomePage(driver);
        profilePage = new ProfilePage(driver);
    }

    public void updateAcademicInformation() throws Exception {

        /* ---------------- OPEN PROFILE ---------------- */

        Thread.sleep(10000);
        log("OPENING PROFILE");

        homePage.openProfile();
        Thread.sleep(5000);

        log("PROFILE OPENED");

        /* ---------------- ACADEMIC DETAILS ---------------- */

        profilePage.scrollToAcademicSection();

        log("ACADEMIC SECTION REACHED");

        /* ---------- STATE ---------- */
        Thread.sleep(5000);
        log("UPDATING STATE");

        profilePage.selectTripura();

        /* ---------- DISTRICT ---------- */

        log("UPDATING DISTRICT");

        profilePage.clickDistrict();

        profilePage.selectFirstDistrict();

        /* ---------- SUB ROLE ---------- */

        log("UPDATING SUB ROLE");

        profilePage.clickSubRole();

        profilePage.selectBEO();

        // profilePage.selectDEO();

        profilePage.clickSubmit();

        /* ---------- SAVE PROFILE ---------- */

        log("SAVING PROFILE");

        profilePage.clickSave();

        log("PROFILE SAVED");
        Thread.sleep(5000);
        driver.navigate().back();

        log("RETURNED TO HOME PAGE");

        /* ---------------- BLOCK TO SCHOOL UPDATE ---------------- */
        Thread.sleep(10000);
        log("OPENING PROGRAM CARD");

        profilePage.clickProgramCard();

        log("PROGRAM CARD OPENED");

        printPageSource();

        profilePage.clickBlockToSchoolUpdate();

        log("BLOCK TO SCHOOL UPDATE OPENED");

        printPageSource();

        /* ---------- BLOCK ---------- */

        profilePage.clickUpdateBlock();

        profilePage.selectBlockAmbassa();

        /* ---------- CLUSTER ---------- */

        profilePage.ClickUpdateCluster();

        profilePage.selectClusterBALARAMHIGHSCHOOL();

        /* ---------- SCHOOL ---------- */

        profilePage.clickUpdateSchool();

        profilePage.selectSchoolBALARAMHIGHSCHOOL();

        /* ---------- SAVE ---------- */

        log("SAVING BLOCK DETAILS");

        profilePage.clickSaveDetails();

        log("PROFILE UPDATE COMPLETED");
    }
}