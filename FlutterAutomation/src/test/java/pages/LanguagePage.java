package pages;

import io.appium.java_client.android.AndroidDriver;
import org.openqa.selenium.By;

import base.BasePage;

public class LanguagePage extends BasePage{

    public LanguagePage(AndroidDriver driver) {
            super(driver);
        }

    public void selectEnglish() throws InterruptedException {

        Thread.sleep(5000);

        System.out.println(
                "Current package = " +
                driver.getCurrentPackage()
        );

        click(
                By.xpath("//*[@content-desc='English']")
        );
    }

    public void clickContinue() {

        click(
            org.openqa.selenium.By.xpath(
                "//*[@content-desc='Continue']"
            )
        );
    }
}