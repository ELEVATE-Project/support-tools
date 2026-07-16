package pages;

import java.util.Map;
import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;


public class ProfilePage extends BasePage{

        public ProfilePage(AndroidDriver driver) {
                super(driver);
        }


        public void scrollToAcademicSection() throws Exception {

                for (int i = 1; i <= 2; i++) {

                        System.out.println("Scroll : " + i);

                        scrollDown();

                        Thread.sleep(2500);
                }
        }
        
        public void scrollToTripura() {

                for (int i = 1; i <= 3; i++) {

                        System.out.println("Scroll : " + i);

                        scrollDown();

                }
                }

        public void clickState() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Select State\")"
                        )
                );
        }

        public void selectTripura() {
                clickState();

                scrollToTripura();
                System.out.println("===== AFTER SCROLL =====");
                
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Tripura\")"
                        )
                );

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Submit\")"
                        )
                );
        }

        public void clickDistrict() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Select District\")"
                        )
                );
        }

        public void selectFirstDistrict() {

                driver.findElements(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Dhalai\")"
                        )
                ).get(0).click();

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Submit\")"
                        )
                );
        }

        public void clickSubRole() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Select Sub Role\")"
                        )
                );
        }

        public void selectBEO() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Block Education Officer (BEO)\")"
                        )
                );
        }

        public void selectDEO() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"District Education Officer (DEO)\")"
                        )
                );
        }

        public void clickSubmit() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Submit\")"
                        )
                );
        }

        public void clickSave() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().description(\"Save\")"
                        )
                );
        }
        
        public void clickProgramCard() {

                click(
                        AppiumBy.xpath("//android.widget.TextView[@text='Programs']/parent::android.view.View")

                        );
        }

        public void clickBlockToSchoolUpdate() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"UPDATE PROFILE\")"
                        )
                );
        }

        private void scrollDownPage() {

                for (int i = 0; i < 6; i++) {

                        driver.executeScript(
                        "mobile: scrollGesture",
                        Map.of(
                                "left", 50,
                                "top", 400,
                                "width", 980,
                                "height", 1500,
                                "direction", "down",
                                "percent", 1.0
                        )
                        );

                        try {
                        Thread.sleep(1200);
                        } catch (Exception e) {
                        }
                }
        }

        public void clickUpdateBlock() {
                
                scrollDownPage();

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Select Block']"
                        )
                );
        }

        public void selectBlockAmbassa() {

                click(
                        AppiumBy.xpath("//android.view.View[@resource-id=\"mat-option-46\"]"
                        )
                );
        }

        public void ClickUpdateCluster() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Select Cluster']"
                        )
                );
        }

        public void selectClusterBALARAMHIGHSCHOOL() {

                click(
                        AppiumBy.xpath("//android.view.View[@resource-id=\"mat-option-56\"]"
                        )
                );
        }

        public void clickUpdateSchool() {

                click(
                        AppiumBy.xpath("//android.widget.Spinner[@text='Select School']"
                        )
                );
        }

        public void selectSchoolBALARAMHIGHSCHOOL() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"mat-option-64\")"
                        )
                );
        }

        public void clickSaveDetails() {

                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"SAVE\")"
                        )
                );
        }
        
}