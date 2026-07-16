package pages;

import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class SurveyPage extends BasePage {

    public SurveyPage(AndroidDriver driver) {
        super(driver);
    }

    public void clickSurvey() {
        click(
                AppiumBy.xpath("//android.webkit.WebView[@text=\"Diksha\"]/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.view.View/android.widget.ListView[2]/android.view.View[3]/android.view.View")
        );
    }

    public void clickSurveyByName(String surveyName) {
        click(
                AppiumBy.xpath("//android.widget.TextView[@text='" + surveyName + "']")
        );
    }

    public void consumeSurveyQ1() {
        type(
                AppiumBy.xpath("//android.widget.EditText[@hint='Enter your response']"),
                "Testing answers"
            );
    }

    public void consumeSurveyQ2() {
        click(
                AppiumBy.xpath("//android.view.View[@resource-id='6a4233ff7c50c8a58c7156c3R1']/android.view.View/android.view.View[1]/android.widget.TextView[3]")
        );
    }

    public void clickPage2() {
        click(
                AppiumBy.xpath("//android.widget.Button[@text='2']")
        );
    }

    public void consumeSurveyQ3() throws Exception {

        click(
                AppiumBy.xpath("//android.widget.Button[@text='Add Student']")
        );

        click(
                AppiumBy.xpath("//android.widget.TextView[@text='Student 1']")
        );

        click(
                AppiumBy.xpath("//android.view.View[@resource-id='mat-mdc-checkbox-3']/android.view.View")
        );

        scrollDown();

        dragSlider(
            AppiumBy.className("android.widget.SeekBar"),
            0.80
        );

        click(
                AppiumBy.xpath("//android.widget.Button[@text='Submit']")
        );
    }

    public void clickPage3() {

        click(
                AppiumBy.xpath("//android.widget.Button[@text='3']")
        );
    }

    public void consumeSurveyQ3P3() throws InterruptedException {

        click(
                AppiumBy.xpath("//android.widget.Button[@content-desc='Open calendar']")
        );

        click(
                AppiumBy.xpath("//android.widget.ToggleButton[@text='3 July 2026']")
        );
    }

    public void submitSurvey() {

        click(
                AppiumBy.xpath("//android.widget.Button[@text='Submit']")
        );

        click(
                AppiumBy.xpath("//android.widget.Button[@text='Confirm']")
        );
    }
}