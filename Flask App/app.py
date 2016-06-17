'''
Created on Jun 15, 2016

@author: chris
'''
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash
from contextlib import closing
app = Flask(__name__)
import datetime
import random
import StringIO
import base64
from flask import Flask, render_template, send_file
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib
from matplotlib.dates import DateFormatter
import MainFast
import tempfile
file_location = r'/home/chris/UROP Data/'

@app.route('/', methods=['POST','GET'])
def images():
    
#     country = request.form['title']
#     file_name = file_location + 'baci92_'
#     plt = MainFast.single_country_run(file_name, False, False, country)
#     
#     f = tempfile.NamedTemporaryFile(
#     dir='templates/static/temp',
#     suffix='.png',delete=False)
#     # save the figure to the temporary file
#     plt.savefig(f)
# #     f.close() # close the file
#     # get the file's name
#     # (the template will need that)
# #     img64 = base64.b64encode(f.read()).decode('UTF-8')
# 
#     plot_name = f.name.split('/')[-1]
# 
#     print(plot_name)

    try:
        title = request.form['title']
    except:
        title = "/"
    try:
        local_shift = request.form['local_shift']
    except:
        local_shift = 1.5
    try:
        absolute_value = request.form['absolute']
    except:
        absolute_value = 50000    
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
        
    return render_template("index.html",title = title, local = local_shift,absolute =absolute_value,market = market_value,trend=trend_value,volatility=volatility_value)    
@app.route('/simple/<country>/', methods=['POST','GET'])
@app.route('/simple/<country>/<local>', methods=['POST','GET'])
@app.route('/simple/<country>/<local>/<absolute>', methods=['POST','GET'])
@app.route('/simple/<country>/<local>/<absolute>/<market>', methods=['POST','GET'])
@app.route('/simple/<country>/<local>/<absolute>/<market>/<trend>', methods=['POST','GET'])
@app.route('/simple/<country>/<local>/<absolute>/<market>/<trend>/<volatility>', methods=['POST','GET'])

def simple(country,local = None,absolute= None, market =None, trend = None, volatility = None):

    file_name = file_location + 'baci92_'
    fig = MainFast.single_country_run(file_name, False, False, country,local,absolute,market,trend,volatility)
    img = StringIO.StringIO()
    fig.savefig(img)
    img.seek(0)
    response = send_file(img, mimetype='image/png')

    return response

#     
#     fig=Figure()
#     fig.set_size_inches(17, 10.5)
# 
#     ax=fig.add_subplot(111)
#     x=[]
#     y=[]
#     now=datetime.datetime.now()
#     delta=datetime.timedelta(days=1)
#     for i in range(10):
#         x.append(now)
#         now+=delta
#         y.append(random.randint(0, 1000))
#     ax.plot_date(x, y, '-')
#     ax.xaxis.set_major_formatter(DateFormatter('%Y-%m-%d'))
#     fig.autofmt_xdate()
#     fig.tight_layout()
#     canvas=FigureCanvas(fig)
#     png_output = StringIO.StringIO()
#     canvas.print_png(png_output)
#     response=make_response(png_output.getvalue())
#     response.headers['Content-Type'] = 'image/png'
#     return response
    
def png(fig):
    """
    Returns a StringIO PNG plot for the sensor
    """
    from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
    canvas = FigureCanvas(fig)
    try:
        png_output = StringIO()
    except NameError:
        png_output = BytesIO()
    canvas.print_png(png_output)
    return png_output.getvalue()
if __name__ == "__main__":
    app.run()