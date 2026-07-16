# DIKSHA Mobile Automation Framework

## Overview

This project is an end-to-end mobile automation framework developed for the **DIKSHA Android Application** using **Appium**, **Java**, **TestNG**, and the **Page Object Model (POM)** design pattern.

The framework automates major user journeys including:

- User Registration
- User Login
- Profile Update
- Project Consumption
- Survey Consumption
- Observation WR
- Observation WOR

The framework is designed to be modular, reusable, scalable, and easy to maintain.

---

# Technology Stack

| Technology | Version |
|------------|---------|
| Java | JDK 17 |
| Maven | 3.x |
| Appium | 3.x |
| Appium Java Client | Latest Compatible |
| Selenium | 4.x |
| TestNG | Latest |
| Android Studio | Latest |
| Android SDK | API 31+ |
| Git | Latest |

---

# Project Structure

```
FlutterAutomation
│
├── src
│   ├── main
│   │
│   └── test
│       ├── java
│       │
│       ├── base
│       │     BasePage.java
│       │     BaseFlow.java
│       │     BaseTest.java
│       │
│       ├── pages
│       │     LoginPage.java
│       │     ProjectPage.java
│       │     SurveyPage.java
│       │     ProfilePage.java
│       │     RegistrationPage.java
│       │     ObservationWRPage.java
│       │     ObservationWORPage.java
│       │
│       ├── flows
│       │     LoginFlow.java
│       │     RegistrationFlow.java
│       │     ProjectFlow.java
│       │     SurveyFlow.java
│       │     ObservationWRFlow.java
│       │     ObservationWORFlow.java
│       │     ProfileFlow.java
│       │
│       ├── tests
│       │     TestUserOnboarding.java
│       │     TestContentConsumption.java
│       │
│       └── data
│             TestData.java
│
├── pom.xml
└── README.md
```

---

# Prerequisites

Ensure the following software is installed before running the automation.

## Java

Install JDK 17

Verify:

```bash
java -version
```

---

## Maven

Install Maven.

Verify:

```bash
mvn -version
```

---

## NodeJS

Install NodeJS LTS.

Verify:

```bash
node -v
npm -v
```

---

## Appium

Install Appium globally.

```bash
npm install -g appium
```

Verify installation:

```bash
appium -v
```

---

## Android Studio

Install Android Studio.

Ensure the following SDK components are installed:

- Android SDK
- Platform Tools
- Build Tools
- Emulator (Optional)

---

## Android Device

Enable:

- Developer Options
- USB Debugging

Verify device:

```bash
adb devices
```

Expected output:

```
List of devices attached

RZXXXXXXXXXX    device
```

---

# Environment Variables

Configure:

```
ANDROID_HOME
ANDROID_SDK_ROOT
JAVA_HOME
```

Example:

```
ANDROID_HOME=/home/user/Android/Sdk

JAVA_HOME=/usr/lib/jvm/java-17-openjdk
```

Add platform-tools to PATH.

Verify:

```bash
adb version
```

---

# Clone Repository

```bash
git clone <repository-url>

cd FlutterAutomation
```

---

# Install Dependencies

```bash
mvn clean install
```

or

```bash
mvn test-compile
```

---

# Appium Server

Start Appium.

```bash
appium
```

Default server:

```
http://127.0.0.1:4723
```

---

# Running Automation

## User Registration + Profile Update

```bash
mvn test -Dtest=TestUserOnboarding
```

---

## Content Consumption

```bash
mvn test -Dtest=TestContentConsumption
```

---

## Run Complete Test Suite

```bash
mvn test
```

---

# Test Data

All configurable test data is stored inside:

```
data/TestData.java
```

Example:

```
Email

Password

Program Name

Project Name

Survey Name

Observation WR Name

Observation WOR Name
```

Update the values before executing the automation.

---

# Framework Design

The framework follows the Page Object Model (POM).

```
Tests

↓

Flows

↓

Pages

↓

BasePage

↓

Appium Driver
```

### BasePage

Contains reusable utilities:

- click()
- type()
- scrollDown()
- dragSlider()
- waitForVisibility()
- waitForClickable()

---

### BaseFlow

Contains:

- Common Logging
- Page Source Printing
- Shared Driver

---

### Pages

Each page contains only UI interaction methods.

Example:

```
clickLogin()

enterEmail()

clickSubmit()

selectSurvey()

clickObservation()
```

---

### Flows

Flows contain business logic.

Example:

```
Login

↓

Project

↓

Survey

↓

Observation
```

---

# Logging

Execution logs are printed for every major action.

Example:

```
==================================================
 LOGIN SUCCESSFUL
==================================================

==================================================
 PROJECT STARTED
==================================================

==================================================
 SURVEY COMPLETED
==================================================
```

---

# Troubleshooting

## Device Not Found

Run:

```bash
adb devices
```

Reconnect the device if not detected.

---

## Appium Session Not Starting

Check:

```
Appium Server Running

USB Debugging Enabled

Correct appPackage

Correct appActivity
```

---

## Element Not Found

Verify:

- Locator
- Page Source
- Appium Inspector

---

## WebView Issues

Print contexts:

```java
System.out.println(driver.getContextHandles());
```

Switch if required:

```java
driver.context("WEBVIEW_xxx");
```

---

# Best Practices

- Avoid Thread.sleep()
- Use Explicit Waits
- Keep Locators inside Page Classes
- Keep Business Logic inside Flow Classes
- Keep Tests clean
- Reuse BasePage methods
- Use descriptive logs

---

# Future Enhancements

- Extent Reports
- Screenshot Capture on Failure
- Jenkins Integration
- Parallel Execution
- Data Driven Testing
- Excel Test Data
- Allure Reporting
- GitHub Actions CI/CD

---

# Author

**Bharath R**

Automation Framework for DIKSHA Mobile Application