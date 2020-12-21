# resalCodingEx
# FastApi-Top-Rating-products

Python implementation of fast api to process uploaded csv and produce a json outlining the top rating products along with their rating score

## Installation

Use the package manager [pip](https://pip.pypa.io/en/stable/) to install fast api.

```bash
pip install fastapi
pip install uvicorn
```
# Unit test setup
```bash
pip install -U pytest
```
```bash
pytest -q csvtojsonapi.py 
```
Note: unit test require the uvcorn server running the application.  

unit test results
![Alt text](images/unitTest.png?raw=true "UnitTest")

## Manual Testing
Some tests are not easy to put into unit test, this is a normal part of every application as they can be covered in automated tests but in out case we just used manual testing to make sure the application is behaving as expected.

Here are the cases,
1. non csv file:
![Alt text](images/ManualTest1.png?raw=true "non-csv exception")
2. empty csv file:
![Alt text](images/ManualTest2.png?raw=true "empty csv exception")
3. missing header csv file:
![Alt text](images/ManualTest3.png?raw=true "corrupt csv file exception")

## Usage

```bash
uvicorn csvtojsonapi:app --reload
```
Once the server is running, then you can go to the swagger UI by following the following: 
```
http://<ip>:8000/docs
```
## Kafka Implementation of the top rating products
Kafka was used to implement the same functionality as in the fastapi to stream the CSV using a standard Kafka producer and a consumer was developed to process the messages on the other end. 
Note: the functionality of processing the data in the fastapi implementation was leveraged as well in this solution.

## Installation
install kafka
```bash
pip install confluent-kafka
```
download and follow instruction to run the kafka server:
```
https://kafka.apache.org/quickstart
```

## Usage
Run the kafka server as instructed in the previous step, then

```bash
python3 producer.py <= enter the csv file path then run
python3 consumer.py 

```
example output:
```
python3 consumer.py
Processing file : in.csv
Done processing file : in.csv
{
    "top_product(s)": "0    Massoub gift card\n1     Kebdah gift card",
    "product_rating(s)": "0    5.4\n1    5.4"
}
```
