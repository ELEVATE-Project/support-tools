package pages;

import java.util.Map;

import org.openqa.selenium.By;

import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class RegistrationPage extends BasePage{

        public RegistrationPage(AndroidDriver driver) {
                super(driver);
        }

         // First Name
        public void enterFirstName(String firstName) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"firstname\")"),
                                firstName
                );
        }

        // Middle Name
        public void enterMiddleName(String middleName) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"middlename\")"),
                                middleName
                );
        }

        // Last Name
        public void enterLastName(String lastName) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"lastname\")"),
                                lastName
                );
        }

        // Date of Birth Field
        public void clickDateOfBirth() {
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"Select Date of Birth\")"
                        )
                );
        }

        public void selectDOB2000() throws InterruptedException {

                clickDateOfBirth();
                Thread.sleep(2000);

                System.out.println("===== DATE PICKER OPEN =====");

                // Open year picker
                click(
                        AppiumBy.id("android:id/date_picker_header_year")
                );

                Thread.sleep(1000);

                System.out.println("===== YEAR PICKER OPEN =====");

                // Scroll year list until 2000 is visible
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiScrollable(new UiSelector()" +
                                ".resourceId(\"android:id/date_picker_year_picker\"))" +
                                ".scrollIntoView(new UiSelector().text(\"2012\"))"
                        )
                );

                System.out.println("===== YEAR 2015 SELECTED =====");

                Thread.sleep(1000);

                // Navigate to January 2015
                while (!driver.getPageSource().contains("January 2012")) {

                click(
                        AppiumBy.xpath(
                                "//android.widget.ImageButton[@content-desc=\"Previous month\"]"
                        )
                );

                Thread.sleep(1000);
                }

                System.out.println("===== JANUARY 2012 REACHED =====");
                Thread.sleep(2000);
                // Select 1 Jan 2015
                click(
                        AppiumBy.xpath(
                                "//android.view.View[@content-desc=\"01 January 2012\"]"
                        )
                );

                System.out.println("===== DATE SELECTED =====");

                // Click SET
                click(
                        AppiumBy.id("android:id/button1")
                );

                System.out.println("===== DATE SETTING DONE =====");
        }


        private void scrollUntilVisible(By locator) {

                int attempts = 0;

                while (driver.findElements(locator).isEmpty() && attempts < 8) {

                        driver.executeScript(
                                "mobile: scrollGesture",
                                Map.of(
                                        "left", 0,
                                        "top", 300,
                                        "width", 1080,
                                        "height", 1600,
                                        "direction", "down",
                                        "percent", 0.7
                                )
                        );

                        attempts++;
                }
        }

        // Role Dropdown
        public void clickRoleDropdown() {
                scrollUntilVisible(
                        AppiumBy.androidUIAutomator(
                        "new UiSelector().resourceId(\"role\")"
                        )
                );
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"role\")"
                        )
                );
        }

        // Select Role from dropdown
        public void selectRole() {
                clickRoleDropdown();
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"Head teacher & Official\")"
                        )
                );
        }

        // Select Gender from dropdown
        public void selectGenderdropdown() {
                scrollUntilVisible(
                        AppiumBy.androidUIAutomator(
                        "new UiSelector().resourceId(\"gender\")"
                        )
                );
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"gender\")"
                        )
                );
        }
        
        public void selectGender() {
                scrollUntilVisible(
                        AppiumBy.androidUIAutomator(
                        "new UiSelector().resourceId(\"gender\")"
                        )
                );
                selectGenderdropdown();
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"Male\")"
                                )
                        );
                }

        public void selectEmail() {
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"emailLbl\")"
                        )
                );
        }

        public void clickGenOTP() {
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"Generate OTP\")"
                        )
                );
        }
        
        public void enterEmail(String email) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"withemail\")"
                        ), email
                );
        }

        public void enterPassword(String password) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"signup-form-password\")"
                        ), password
                );
        }

        public void enterConfirmPassword(String confirmPassword) {
                type(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"signup-form-confirmPassword\")"
                        ), confirmPassword
                );
        }

        public void clickAgree() {
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().resourceId(\"flexCheckChecked\")"
                        )
                );
        }

        // Register / Submit Button
        public void clickRegister() {
                click(
                        AppiumBy.androidUIAutomator(
                                "new UiSelector().text(\"Continue\")"
                        )
                );
        }
}