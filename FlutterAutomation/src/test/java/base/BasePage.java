package base;

import java.time.Duration;
import java.util.Map;

import org.openqa.selenium.By;
import org.openqa.selenium.Rectangle;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.support.ui.ExpectedConditions;
import org.openqa.selenium.support.ui.WebDriverWait;

import io.appium.java_client.android.AndroidDriver;

public class BasePage {

    protected AndroidDriver driver;
    protected WebDriverWait wait;

    public BasePage(AndroidDriver driver) {
        this.driver = driver;
        this.wait = new WebDriverWait(driver, Duration.ofSeconds(30));
    }

    protected WebElement waitForVisibility(By locator) {
        return wait.until(
                ExpectedConditions.visibilityOfElementLocated(locator));
    }

    protected WebElement waitForClickable(By locator) {
        return wait.until(
                ExpectedConditions.elementToBeClickable(locator));
    }

    /* ---------- Common Actions ---------- */

    protected void click(By locator) {
        waitForClickable(locator).click();
    }

    protected void type(By locator, String text) {
        WebElement element = waitForVisibility(locator);
        element.clear();
        element.sendKeys(text);
    }

    protected String getText(By locator) {
        return waitForVisibility(locator).getText();
    }

    protected boolean isDisplayed(By locator) {
        try {
            return waitForVisibility(locator).isDisplayed();
        } catch (Exception e) {
            return false;
        }
    }

    protected void scrollDown() {

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
    }

    protected void dragSlider(By locator, double percentage) {

        WebElement slider = waitForVisibility(locator);

        Rectangle rect = slider.getRect();

        int startX = rect.getX() + 5;
        int endX = rect.getX() + (int) (rect.getWidth() * percentage);
        int y = rect.getY() + rect.getHeight() / 2;

        driver.executeScript(
                "mobile: dragGesture",
                Map.of(
                        "startX", startX,
                        "startY", y,
                        "endX", endX,
                        "endY", y
                )
        );
    }
}