#!/usr/bin/env python3.11
import sys
import os

# Add the project root directory to the Python path
# This allows us to use absolute imports like 'from src.module import ...'
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from flask import Flask, render_template

# Import the blueprint from strata.py
from strata import strata_bp

# Initialize Flask app
# template_folder points to 'src/templates' relative to this file's location (src/)
# static_folder points to 'src/static'
app = Flask(__name__, template_folder='templates', static_folder='static')

# Register the blueprint with the /api prefix, as expected by index.html
app.register_blueprint(strata_bp, url_prefix='/api')

@app.route('/')
def serve_index():
    """Serves the main index.html page."""
    return render_template('index.html')

if __name__ == '__main__':
    # Run on 0.0.0.0 to be accessible. Port changed to 5008.
    app.run(host='0.0.0.0', port=80, debug=False)