from typing import Optional
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.testclient import TestClient
import pandas as pd
import json
from pathlib import Path
import pytest

app = FastAPI()


@app.get("/")
def read_root():
    return {"Hello": "World"}

def sanitize_helper(x):
	''' 
	This function helps sanitizing data in the csv rating column

	Arguuments:
	x --  the content of the rating column

	Return: 
	x if x is a float 
	0 otherwise 
	'''
	try:
	 float(x)
	 return float(x)
	except:
	 return 0

def find_best_rating_products(csv_file = None):
	'''
	This function represnts the engine behind processing the csv file from the user and producing a dataframe containing the
	top rated products and rating score.

	Keyword arguments:
	csv_file -- represent the csv file uploaded by the user

	Return:
	dataframe containing the top rated products and rating score.
	'''
	data = pd.read_csv(csv_file)
	rating_column = data["customer_avrage_rating"].map(sanitize_helper)
	max_rating_value = rating_column.max()
	res = data.loc[data["customer_avrage_rating"] == str(max_rating_value)]
	return res


@app.post("/top-rating-products")
async def top_rating_products(csv_file: UploadFile = File(...)):
	'''
	This function receives uploaded user files through the end point
	/top-rating-products. 

	Keyword arguments:
	csv_file -- represent the file object uploaded by the user

	Return:
	result of finding the top rated products along with there ratings or 
	an exception in the floowing cases:
	1. Function failed to process the user file.
	2. The file is not a .csv file.
	'''
	if csv_file.filename.endswith('.csv'):
	 try:
	  result = find_best_rating_products(csv_file = csv_file.file)
	  return {"top_product(s)" : result["product_name"], "product_rating(s)" : result['customer_avrage_rating']}
	 except BaseException as e:
	  raise HTTPException(status_code=400, detail={"Error": "problem reading/opening/processing file {}".format(csv_file.filename), "Exception type": type(e).__name__, "Exception args": e.args})
	else:
	 raise HTTPException(status_code=400, detail= {'Error': "filename [{}] is not a supported file format".format(csv_file.filename)})


## Unit Testing Section
client = TestClient(app)

# set up for the files used in testing
empty_csv = Path(__file__).parent / "./resources/empty.csv"
only_headers_csv = Path(__file__).parent / "./resources/onlyheaders.csv"
date_csv_filename = Path(__file__).parent / "./resources/in.csv"


def test_return_max_customer_rating_no_headers_no_file():
	'''
	Testing the fast api to make sure the csv file is provided.
	'''
	response = client.post("/top-rating-products")
	assert response.status_code == 422
	assert response.json() == {"detail":[{"loc":["body","csv_file"],"msg":"field required","type":"value_error.missing"}]}

def test_return_max_customer_rating_no_file():
	'''
	Tetsing the missing file again but with headers this time, 
	this test is to set exceptations on what is returned with we have headers and no file

	Note:
	This test needs the server running otherwise it is going to fail
	'''
	response = client.post("/top-rating-products",
		headers={"accept": "application/json", "Content-Type": "multipart/form-data"})
	assert response.status_code == 400
	assert response.json() == {'detail': 'There was an error parsing the body'}

def test_find_best_rating_products_with_file_not_sane_data():
	'''
	testing the returned json when we have a file with a mix of data types in the rating.
	Note:
	This test needs the server running otherwise it is going to fail.
	'''
	actual =  {"id": [132, 154], "product_name": ["Massoub gift card", "Kebdah gift card"], "customer_avrage_rating": ["5.4", "5.4"]}
	actual = pd.DataFrame(data=actual)
	expected = find_best_rating_products(csv_file = date_csv_filename)
	print(type(expected["customer_avrage_rating"][0]))
	print(actual.to_string())
	assert True == actual.equals(expected)

def test_find_best_rating_products_with_headers_only_csv():
	'''
	testing that the fuctions still process a header only file and return an empty result.
	'''
	expected = find_best_rating_products(csv_file = only_headers_csv)
	assert True == expected.empty

def test_sanitize_helper():
	'''
	testing the helper function that makes sure we sanitiaze the rating column to discard non numeric values.
	'''
	x = 10.1
	assert x == sanitize_helper(x)
	x = None
	assert 0 == sanitize_helper(x)
	x = -1
	assert x == sanitize_helper(x)
	x = 'ali'
	assert 0 == sanitize_helper(x)
	x = float('NaN')
	assert 0 -- sanitize_helper(x)
