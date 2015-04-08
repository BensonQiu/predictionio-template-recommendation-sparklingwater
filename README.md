## Sparkling Water-Deep Learning Engine Template

This engine template has integrated Sparkling Water's Deep Learning Model by default.

#### Overview
This Engine Template demonstrates an energy forecasting engine. It integrates Deep Learning from the Sparkling Water library to perform energy analysis. We can query the circuit and time, and return predicted energy usage. 

#### Usage
**Event Data Requirements**

By default, the engine requires the following events to be collected:
* Circuit ID
* Time
* Energy

**Input Query**
* Circuit ID
* Time

**Output PredictedResult**
* Energy Consumption

#### 1. Run PredictionIO

If PredictionIO is not installed, install it [here](http://docs.prediction.io/install/).

Start all components (Event Server, Elaticsearch, and HBase).

Note: If `pio-start-all` is not recognized, upgrade to the latest version of PredictionIO.
```
$ pio-start-all
```

Verify the status of components:
```
$ pio status
```

#### 2. Download the Engine Template

```
git clone https://github.com/BensonQiu/predictionio-template-recommendation-sparklingwater
```

#### 3. Create a new application
```
$ pio app new [YourAppName]
```

The console output should include the App Name, **App ID**, and **Access Key**. You will need the App ID and Access Key in future steps. You can view your applications by entering `pio app list`.

#### 4.Import Data to the Event Server

Install the PredictionIO Python SDK:
```
$ pip install predictionio
```
or
```
$ easy_install predictionio
```

From the root directory of your engine, run:
```
$ python data/import_eventserver.py --access_key [YourAccessKeyFromStep3]
```

#### 5. Build, Train, and Deploy the Engine

From the root directory of your engine, find `engine.json` and verify that the appId matches the **App Id** of your application from Step 3.

```
 ...
  "datasource": {
    "params" : {
      "appId": 1
    }
  },
  ...
```

Build the engine.
```
$ pio build
```

Train the engine. This will take several minutes.
```
$ pio train
```

Deploy the engine. This will take several minutes.
```
$ pio deploy
```

After deploying successfully, you can view the status of your engine at [http://localhost:8000](http://localhost:8000).

#### 6. Using the Engine
To do a sample query, run `python query.py` from the root directory of your engine. Customize the query by modifying the JSON `{ circuitId: 1, time: "1422985500" }` in `query.py`. The engine will return a JSON object containing predicted energy usage.
