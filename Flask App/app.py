'''
Created on Jun 15, 2016

@author: chris
'''
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash
from io import BytesIO

from contextlib import closing
import Error_Handler

app = Flask(__name__)



import datetime
import random
import base64
from flask import Flask, render_template, send_file
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib
from matplotlib.dates import DateFormatter
import tempfile
import mpld3
import json
import Trend_Handler
import Error_Handler
import Plotter
# file_location = r'/home/chris/UROP Data/'
file_location = r'C:/Users/Chris Briere/UROP Data/'
used_codes_dict = {} # [country] = [errors] 
# @app.route('/', methods=['POST','GET'])
# def hello_world():
#     return render_template("index-simple.html")
@app.route('/errors', methods=['POST','GET'])
def display_errors():
    try:
        title = request.form['title']
    except:
        title = "Insert Country Here"
    try:
        local_shift = request.form['local_shift']
    except:
        local_shift = 1.5
    try:
        absolute_value = request.form['absolute']
    except:
        absolute_value = .0001    
    try:
        market_value = request.form['market']
    except:
        market_value = .15
    try:
        trend_value = request.form['trend']
    except:
        trend_value = 1   
    try:
        volatility_value = request.form['volatility']
    except:
        volatility_value = 3 
    return render_template("errors.html", title = title, local = local_shift,absolute =absolute_value,market = market_value,trend=trend_value,volatility=volatility_value)    

@app.route('/errors_img/<country>/', methods=['POST','GET'])
@app.route('/errors_img/<country>/<local>', methods=['POST','GET'])
@app.route('/errors_img/<country>/<local>/<absolute>', methods=['POST','GET'])
@app.route('/errors_img/<country>/<local>/<absolute>/<market>', methods=['POST','GET'])
@app.route('/errors_img/<country>/<local>/<absolute>/<market>/<trend>', methods=['POST','GET'])
@app.route('/errors_img/<country>/<local>/<absolute>/<market>/<trend>/<volatility>', methods=['POST','GET'])
 
def errors(country,local = None,absolute= None, market =None, trend = None, volatility = None):
 
    print (country,local,absolute,market,trend,volatility)
    file_name = file_location + 'baci92_'

    plt = Error_Handler.getErrorsPlot(file_name, country,local,absolute,market,trend,volatility)


    f = tempfile.NamedTemporaryFile(
    dir= tempfile.gettempdir(),
    suffix='.png',delete=False)

    plt.savefig(f)

    f.seek(0)
    response =  send_file(f, mimetype='image/png')
#     f.close() # close the file
    return response

@app.route('/countries', methods=['POST','GET'])
def display_countries():
    try:
        title = request.form['title']
    except:
        title = "Insert Country Here"
    try:
        relative = request.form['relative']
    except:
        relative = 1.5
    try:
        absolute_value = request.form['absolute']
    except:
        absolute_value = 50000    
    try:
        market_value = request.form['market']
    except:
        market_value = .15
    try:
        trend_value = request.form['trendline']
    except:
        trend_value = 1   

    return render_template("countries.html", title = title, relative = relative,absolute =absolute_value,market = market_value,trendline=trend_value)    


@app.route('/country_img/<country>/', methods=['POST','GET'])
@app.route('/country_img/<country>/<relative>', methods=['POST','GET'])
@app.route('/country_img/<country>/<relative>/<absolute>', methods=['POST','GET'])
@app.route('/country_img/<country>/<relative>/<absolute>/<market>', methods=['POST','GET'])
@app.route('/country_img/<country>/<relative>/<absolute>/<market>/<trendline>', methods=['POST','GET'])

 
def country_trends(country,relative = None,absolute= None, market = None, trendline = "All"):
 
    print (country,relative,absolute,market,trendline)
    file_name = file_location + 'baci92_'
    
    print (trendline)
    
    plt = Trend_Handler.getTrendsPlot(file_name, country,False,relative,absolute,market,trendline)


    f = tempfile.NamedTemporaryFile(
    dir= tempfile.gettempdir(),
    suffix='.png',delete=False)

    plt.savefig(f)

    f.seek(0)
    response =  send_file(f, mimetype='image/png')
#     f.close() # close the file
    return response
    
@app.route('/products', methods=['POST','GET'])
def display_products():
    try:
        title = request.form['title']
    except:
        title = "Insert Country Here"
    try:
        relative = request.form['relative']
    except:
        relative = 1.5
    try:
        absolute_value = request.form['absolute']
    except:
        absolute_value = 50000    
    try:
        market_value = request.form['market']
    except:
        market_value = .15
    try:
        trend_value = request.form['trendline']
    except:
        trend_value = 1   
#     print (trend_value)
    
    return render_template("products.html", title = title, relative = relative,absolute =absolute_value,market = market_value,trendline=trend_value)    


@app.route('/product_img/<country>/', methods=['POST','GET'])
@app.route('/product_img/<country>/<relative>', methods=['POST','GET'])
@app.route('/product_img/<country>/<relative>/<absolute>', methods=['POST','GET'])
@app.route('/product_img/<country>/<relative>/<absolute>/<market>', methods=['POST','GET'])
@app.route('/product_img/<country>/<relative>/<absolute>/<market>/<trendline>', methods=['POST','GET'])

def product_trends(country,relative = None,absolute= None, market =None,trendline = "All"):
 
    print (country,relative,absolute,market)
    file_name = file_location + 'baci92_'

    plt = Trend_Handler.getTrendsPlot(file_name, country,True,relative,absolute,market,trendline)


    f = tempfile.NamedTemporaryFile(
    dir= tempfile.gettempdir(),
    suffix='.png',delete=False)

    plt.savefig(f)

    f.seek(0)
    response =  send_file(f, mimetype='image/png')
#     f.close() # close the file
    return response

@app.route('/products_list', methods=['POST','GET'])
def display_products_list():
    try:
        title = request.form['title']
    except:
        title = "Insert Country Here"
    try:
        relative = request.form['relative']
    except:
        relative = 1.5
    try:
        absolute_value = request.form['absolute']
    except:
        absolute_value = 50000    
    try:
        market_value = request.form['market']
    except:
        market_value = .15
    try:
        trend_value = request.form['trendline']
    except:
        trend_value = 1   
    try:
        type = request.form['type']
    except:
        type = "All"    
    
    #     print (trend_value)

    file_name = file_location + 'baci92_'

    (main_val,printout_val) = Trend_Handler.getTrendsList(file_name, title,True,relative,absolute_value,market_value,trend_value,type)

    return render_template("products_list.html", title = title, relative = relative,absolute =absolute_value,market = market_value,trendline=trend_value,printout=printout_val, main = main_val)    


# @app.route('/product_list/<country>/', methods=['POST','GET'])
# @app.route('/product_list/<country>/<relative>', methods=['POST','GET'])
# @app.route('/product_list/<country>/<relative>/<absolute>', methods=['POST','GET'])
# @app.route('/product_list/<country>/<relative>/<absolute>/<market>', methods=['POST','GET'])
# @app.route('/product_list/<country>/<relative>/<absolute>/<market>/<trendline>', methods=['POST','GET'])
# 
# def product_trends_list(country,relative = None,absolute= None, market =None,trendline = "All"):
#  
#     print (country,relative,absolute,market)
#     file_name = file_location + 'baci92_'
# 
#     list = Trend_Handler.getTrends(file_name, country,True,relative,absolute,market,trendline)
# 
#     return list
@app.route('/plot/<country>/', methods=['POST','GET'])
@app.route('/plot/<country>/<inputs>', methods=['POST','GET'])
 
def plot(country,inputs):
 
    args = []
    print (inputs)
    values = inputs.strip("}").strip("{").split(" ")
    for i in values:
        print (i)
        out = i.split(":")[0].strip("'")
        if (out != i): 
            args.append(out.split(","))
            print (out.split(","))
    plt = Plotter.plot(inp_country = country,inputs = args)
 
    f = tempfile.NamedTemporaryFile(
    dir= tempfile.gettempdir(),
    suffix='.png',delete=False)
 
    plt.savefig(f)
 
    f.seek(0)
    response =  send_file(f, mimetype='image/png')
#     f.close() # close the file
    return response
if __name__ == "__main__":
    app.run(debug=True)