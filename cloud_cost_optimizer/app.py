from flask import Flask, render_template, request, redirect, url_for, session, flash, Response, jsonify
from pymongo import MongoClient
import pandas as pd
import os
import io
import json
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename

app = Flask(__name__)
app.secret_key = "super_secret_cost_optimizer_key"
app.config['UPLOAD_FOLDER'] = 'uploads'
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# MongoDB Setup
try:
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=5000)
    db = client['cloud_cost_db']
    users_collection = db['users']
    data_collection = db['cloud_data']
except Exception as e:
    print(f"MongoDB connection error: {e}")

# Helpers
def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in {'csv'}

# Ensure user is logged in
def login_required(f):
    def wrap(*args, **kwargs):
        if 'user' in session:
            return f(*args, **kwargs)
        else:
            flash("You need to login first", "danger")
            return redirect(url_for('login'))
    wrap.__name__ = f.__name__
    return wrap

# --- MODULE 1: AUTHENTICATION ---

@app.route('/')
def index():
    if 'user' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        
        if users_collection.find_one({'$or': [{'username': username}, {'email': email}]}):
            flash("User already exists with that username or email.", "danger")
            return redirect(url_for('register'))
            
        hashed_password = generate_password_hash(password)
        users_collection.insert_one({'username': username, 'email': email, 'password': hashed_password})
        flash("Registration successful. Please login.", "success")
        return redirect(url_for('login'))
        
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        
        user = users_collection.find_one({'email': email})
        if user and check_password_hash(user['password'], password):
            session['user'] = user['username']
            session['email'] = user['email']
            flash("Logged in successfully.", "success")
            return redirect(url_for('dashboard'))
        else:
            flash("Invalid email or password.", "danger")
            
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for('login'))


# --- MODULE 2 & 3: CSV UPLOAD & DATA CLEANING ---

@app.route('/upload', methods=['POST'])
@login_required
def upload_file():
    if 'file' not in request.files:
        flash('No file part', "danger")
        return redirect(url_for('dashboard'))
    file = request.files['file']
    if file.filename == '':
        flash('No selected file', "danger")
        return redirect(url_for('dashboard'))
    
    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(filepath)
        
        # Data Cleaning & Preprocessing using Pandas
        try:
            df = pd.read_csv(filepath)
            
            # Basic validation of columns
            required_cols = ['Resource_ID', 'Resource_Type', 'Region', 'Status', 'Usage_Hours', 'Cost']
            missing_cols = [c for c in required_cols if c not in df.columns]
            
            if missing_cols:
                flash(f"CSV is missing required columns: {', '.join(missing_cols)}", "danger")
                return redirect(url_for('dashboard'))
                
            # Handle missing values
            df.fillna({'Usage_Hours': 0, 'Cost': 0.0, 'Status': 'Unknown'}, inplace=True)
            
            # Type casting to numeric where needed
            df['Usage_Hours'] = pd.to_numeric(df['Usage_Hours'], errors='coerce').fillna(0)
            df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0.0)
            df['Status'] = df['Status'].str.strip().str.title()
            
            # Store in MongoDB (remove prior data for this user to keep dashboard fresh, or append. We will replace for simplicity)
            data_collection.delete_many({'user': session['user']})
            records = df.to_dict('records')
            
            for doc in records:
                doc['user'] = session['user']
                
            data_collection.insert_many(records)
            flash("File successfully uploaded and processed.", "success")
            
        except Exception as e:
            flash(f"Error processing file: {str(e)}", "danger")
            
        return redirect(url_for('dashboard'))
    else:
        flash("Invalid file format. Please upload a CSV.", "danger")
        return redirect(url_for('dashboard'))


# --- MODULE 4, 5, 6: COST ANALYSIS, OPTIMIZATION & DASHBOARD ---

@app.route('/dashboard')
@login_required
def dashboard():
    # Fetch user data
    user_data = list(data_collection.find({'user': session['user']}, {'_id': 0}))
    
    if not user_data:
        # Render empty dashboard
        return render_template('dashboard.html', 
                               has_data=False, 
                               total_cost=0, 
                               resource_costs={}, 
                               region_costs={},
                               savings=0,
                               recommendations=[])
    
    df = pd.DataFrame(user_data)
    
    # 1. Cost Analysis
    total_cost = df['Cost'].sum()
    
    # Cost by Resource Type
    cost_by_type = df.groupby('Resource_Type')['Cost'].sum().to_dict()
    
    # Cost by Region
    cost_by_region = df.groupby('Region')['Cost'].sum().to_dict()
    
    # 2. Optimization Suggestions
    # Detect idle or stopped resources
    underutilized = df[df['Status'].isin(['Stopped', 'Idle', 'Suspended', 'Terminated'])]
    potential_savings = underutilized['Cost'].sum()
    
    recommendations = []
    
    for _, row in underutilized.iterrows():
        rec = {
            'Resource_ID': row['Resource_ID'],
            'Type': row['Resource_Type'],
            'Status': row['Status'],
            'Cost': row['Cost'],
            'Action': f"Terminate or start {row['Status'].lower()} resource",
            'Savings': row['Cost']
        }
        recommendations.append(rec)
        
    # High cost active resources could also be rightsizing targets
    active_vms = df[(df['Resource_Type'].str.contains('VM|Instance', case=False, na=False)) & (df['Status'] == 'Active')]
    # Let's mock a heavy cost filter to suggest rightsizing
    if not active_vms.empty:
        avg_cost = active_vms['Cost'].mean()
        high_cost_vms = active_vms[active_vms['Cost'] > avg_cost * 1.5]
        for _, row in high_cost_vms.iterrows():
            saving_est = round(row['Cost'] * 0.3, 2) # Assume 30% savings by rightsizing
            rec = {
                'Resource_ID': row['Resource_ID'],
                'Type': row['Resource_Type'],
                'Status': row['Status'],
                'Cost': row['Cost'],
                'Action': "Rightsize resource (downgrade plan)",
                'Savings': saving_est
            }
            recommendations.append(rec)
            potential_savings += saving_est
            
    # Chart Data Preparation (JSON for frontend JS)
    chart_data = {
        'types_labels': list(cost_by_type.keys()),
        'types_values': list(cost_by_type.values()),
        'regions_labels': list(cost_by_region.keys()),
        'regions_values': list(cost_by_region.values())
    }
    
    return render_template('dashboard.html', 
                           has_data=True, 
                           total_cost=round(total_cost, 2),
                           chart_data=json.dumps(chart_data),
                           savings=round(potential_savings, 2),
                           recommendations=recommendations,
                           data_preview=df.head(10).to_dict('records'))

# --- MODULE 7: REPORT GENERATION ---

@app.route('/download_report')
@login_required
def download_report():
    user_data = list(data_collection.find({'user': session['user']}, {'_id': 0, 'user': 0}))
    if not user_data:
        flash("No data to download.", "warning")
        return redirect(url_for('dashboard'))
        
    df = pd.DataFrame(user_data)
    
    # Generate CSV in memory
    output = io.StringIO()
    df.to_csv(output, index=False)
    
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition": "attachment; filename=cloud_cost_report.csv"}
    )
    
if __name__ == '__main__':
    app.run(debug=True, port=5000)
