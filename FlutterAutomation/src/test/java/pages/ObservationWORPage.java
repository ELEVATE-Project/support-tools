package pages;

import java.util.Map;
import base.BasePage;
import io.appium.java_client.AppiumBy;
import io.appium.java_client.android.AndroidDriver;

public class ObservationWORPage  extends BasePage{

        public ObservationWORPage(AndroidDriver driver) {
            super(driver);
        }
        
        public void clickObs() {
            click(
                AppiumBy.xpath(
                    "//android.widget.TextView[@text=\"Observations\"]"
                )
            );
        }

        public void clickObsWOR(String ObservationWORName) {
                click(
                        AppiumBy.xpath(
                                "//android.widget.TextView[@text=\"" + ObservationWORName + "\"]"
                        )
                );
        }

        public void clickAddSchool() {
                click(
                        AppiumBy.xpath(
                                "//android.widget.Button[@text=\"Add school\"]"
                        )
                );
        }

        public void selectSchool(){
            click(
                AppiumBy.xpath("//android.view.View[@text=\"TRIPURA LOKASHIKSHALAYA HIGH SCHOOL, 16011003504\"]"                  
                )
            );

            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Add\"]"
                )
            );
        }
        public void clickStartObservation() {
                click(
                        AppiumBy.xpath(
                                "//android.widget.TextView[@text=\"TRIPURA LOKASHIKSHALAYA HIGH SCHOOL\"]"
                        )
                );
        }

        public void clickObservation1(){
            click(
                AppiumBy.xpath("//android.widget.TextView[@text=\"Observation 1\"]"
                )
            );
        }

        public void consumeFirstQuestion() throws InterruptedException{
            click(
                AppiumBy.xpath("//android.widget.Button[@content-desc=\"Open calendar\"]"
                )
            );
            click(
                AppiumBy.xpath("//android.widget.ToggleButton[@text='3 July 2026']")
            );
        }

        public void consume2Question() {
            type(
                AppiumBy.xpath("//android.widget.EditText[@text='Enter your response']"),
                "9"
            );
        }

        public void consume3Question(){
            click(
                AppiumBy.xpath(
                    "//android.view.View[@resource-id=\"6a4233c57c50c862a27153deR1\"]/android.view.View/android.view.View[1]/android.widget.TextView[3]"
                )
            );
        }

        public void consume5Question(){
            scrollDown();
            click(
                AppiumBy.xpath(
                    "//android.view.View[@resource-id=\"mat-mdc-checkbox-6\"]/android.view.View/android.view.View/android.widget.TextView[3]"
                )
            );

            click(
                AppiumBy.xpath(
                    "//android.view.View[@resource-id=\"mat-mdc-checkbox-7\"]/android.view.View/android.view.View/android.widget.TextView[3]"
                )
            );
        }

        public void consume6Question(){
            scrollDown();
            click(
                AppiumBy.xpath(
                    "//android.view.View[@resource-id=\"6a4233c57c50c8ee717153e0R1\"]/android.view.View/android.view.View[1]/android.widget.TextView[3]"
                )
            );
        }

        public void consume7Question(){
            scrollDown();
            click(
                AppiumBy.xpath(
                    "//android.view.View[@resource-id=\"6a4233c57c50c8b4bf7153d9R1\"]/android.view.View/android.view.View[1]/android.widget.TextView[3]"
                )
            );
        }

        public void consume9Question(){
            scrollDown();
            click(
                AppiumBy.xpath(
                    "//android.view.View[@resource-id=\"6a4233c57c50c80f897153dcR1\"]/android.view.View/android.view.View[1]/android.widget.TextView[3]"
                )
            );
        }

        public void moveToPage2(){
            scrollDown();
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"2\"]"
                )
            );
        }

        private void scrollQ10DownPage() {

                for (int i = 0; i < 1; i++) {

                        driver.executeScript(
                        "mobile: scrollGesture",
                        Map.of(
                                "left", 50,
                                "top", 400,
                                "width", 980,
                                "height", 1500,
                                "direction", "down",
                                "percent", 0.6
                        )
                        );

                        try {
                        Thread.sleep(1200);
                        } catch (Exception e) {
                        }
                }
        }

        public void consume10Question() throws Exception{
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Add Student\"]"
                )
            );

            click(
                AppiumBy.xpath(
                    "//android.widget.TextView[@text=\"Student 1\"]"
                )
            );

            consume10aQuestion();

            scrollQ10DownPage();

            consume10bQuestion();

            scrollQ10DownPage();
            Thread.sleep(10000);
            // consume10cQuestion();

            scrollQ10DownPage();

            consume10dQuestion();

            clickSubmitTask10();
        }

        public void consume10aQuestion(){
            // click(
            //     AppiumBy.xpath(
            //         "//android.widget.TextView[@text=\"10a .  When did you last take a course on Diksha? *\"]"
            //     )
            // );

            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Capture\"]"
                )
            );
        }

        public void consume10bQuestion(){

            dragSlider(
                AppiumBy.className("android.widget.SeekBar"),
                0.80);
        }

        public void consume10cQuestion(){
            type(
                AppiumBy.xpath(
                    "//android.widget.EditText[@resource-id=\"mat-input-5\"]"),
                    "6"
            );
        }

        public void consume10dQuestion(){
            
            type(
                AppiumBy.xpath(
                    "//android.widget.EditText[@resource-id=\"mat-input-6\"]"),
                    "Test Observation WOR Flow"
            );
        }

        public void clickSubmitTask10() {
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Submit\"]"
                )
            );
        }

        public void seecompletedObservationWOR() {
            click(
                AppiumBy.xpath(
                    "//android.view.View[@text='Completed' and @clickable='true']"
                )
            );
        }

        public void confirmsubmit() {
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Confirm\"]"
                )
            );
        }

        public void clickReports() {
            click(
                AppiumBy.xpath(
                    "//android.widget.Button[@text=\"Reports\"]"
                )
            );
        }
}