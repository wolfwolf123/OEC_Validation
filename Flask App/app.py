'''
Created on Jun 15, 2016

@author: chris
'''
from flask import Flask, request, session, g, redirect, url_for, \
     abort, render_template, flash
from contextlib import closing
app = Flask(__name__)
import datetime
import StringIO
import random
from flask import Flask, render_template
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from matplotlib.dates import DateFormatter
import MainFast

file_location = r'/home/chris/UROP Data/'

@app.route('/')
def images():
    return render_template("index.html")
@app.route('/', methods=['POST','GET'])
def simple():
    country = request.form['title']
    file_name = file_location + 'baci92_'
    response = MainFast.single_country_run(file_name, False, False, "eunld")
    return render_template("index.html", graph=response)

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


if __name__ == "__main__":
    app.run()