package pages;

import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class StudentProfilePage extends BasePage{

    public StudentProfilePage(AndroidDriver driver) {
        super(driver);
    }

    public void clickStudent() {

        click(
                AppiumBy.accessibilityId("Student")
        );
    }

    public void clickBoard() {

        click(
                AppiumBy.accessibilityId("Select Board")
        );
    }

    public void clickMedium() {

        click(
                AppiumBy.accessibilityId("Select Medium")
        );
    }

    public void clickClass() {

        click(
                AppiumBy.accessibilityId("Select Class")
        );
    }

    public void clickContinue() {

        click(
                AppiumBy.accessibilityId("Continue")
        );
    }

    public void selectCBSE() {

        click(
                AppiumBy.accessibilityId("CBSE")
        );
    }

    public void clickSubmit() {

        click(
                AppiumBy.accessibilityId("Submit")
        );
    }

    public void selectEnglish() {

        click(
                AppiumBy.accessibilityId("English")
        );
    }

    public void selectClass8() {

        click(
                AppiumBy.accessibilityId("Class 8")
        );
    }

}