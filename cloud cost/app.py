from flask import Flask, render_template, request, redirect, url_for, session, flash, Response
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash
from bson.objectid import ObjectId
import pandas as pd
import io
import csv
import datetime
import os
import glob

# Helper function for fetching data
def get_user_data_and_files():
    query = {}
    if not session.get('is_admin'):
        query['user_id'] = session['user_id']
        
    files = data_col.distinct('source_file', query)
    files = [f for f in files if f]
    
    selected_file = request.args.get('filename')
    if not selected_file and files:
        selected_file = files[-1]
        
    if selected_file:
        query['source_file'] = selected_file
        
    user_data = list(data_col.find(query, {'_id': 0}))
    return user_data, files, selected_file

app = Flask(__name__)
app.secret_key = 'super_secret_key'

# MongoDB connection
client = MongoClient('mongodb://localhost:27017/cloudoptima')
db = client['cloudoptima']
users_col = db['users']
data_col = db['resource_data']

@app.route('/')
def index():
    if 'user_id' in session:
        return redirect(url_for('dashboard'))
    return redirect(url_for('login'))

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username']
        email = request.form['email']
        password = request.form['password']
        confirm_password = request.form.get('confirm_password', '')
        
        if password != confirm_password:
            flash('Registration failed: Passwords do not match.', 'error')
            return redirect(url_for('register'))
            
        if users_col.find_one({'email': email}):
            flash('Registration failed: Email already registered.', 'error')
            return redirect(url_for('register'))
        
        hashed_pw = generate_password_hash(password)
        # First user is admin, else normal user (or could check specific email)
        is_admin = users_col.count_documents({}) == 0 or email.lower() == 'admin@cloudoptima.com'
        
        users_col.insert_one({
            'username': username,
            'email': email,
            'password': hashed_pw,
            'is_admin': is_admin
        })
        flash('Account created successfully.', 'success')
        return redirect(url_for('login'))
        
    return render_template('register.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        email = request.form['email']
        password = request.form['password']
        
        user = users_col.find_one({'email': email})
        if user and check_password_hash(user['password'], password):
            session['user_id'] = str(user['_id'])
            session['username'] = user['username']
            session['is_admin'] = user.get('is_admin', False)
            flash('Logged in successfully.', 'success')
            if session['is_admin']:
                return redirect(url_for('admin_dashboard'))
            return redirect(url_for('dashboard'))
        
        flash('Invalid credentials', 'error')
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    flash('Logged out successfully.', 'success')
    return redirect(url_for('login'))

@app.route('/upload', methods=['GET', 'POST'])
def upload():
    if 'user_id' not in session:
        return redirect(url_for('login'))
        
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('Upload failed: No file selected.', 'error')
            return redirect(request.url)
            
        file = request.files['file']
        if file.filename == '':
            flash('Upload failed: No file selected.', 'error')
            return redirect(request.url)
            
        if file and file.filename.endswith('.csv'):
            try:
                # Read CSV using pandas
                df = pd.read_csv(file)
                
                # Basic data cleaning
                df.dropna(how='all', inplace=True)
                
                if df.empty:
                    flash('Upload failed: The provided CSV file is empty.', 'error')
                    return redirect(request.url)

                # Required columns mapping (assuming standard columns if missing)
                df.columns = [c.strip().replace(' ', '_') for c in df.columns]

                # Check for minimum required columns (e.g., Cost)
                required_cols_check = ['Resource_ID', 'Resource_Type', 'Cost']
                missing_cols = [col for col in required_cols_check if col not in df.columns]
                
                if missing_cols:
                    flash(f"Upload failed: Missing required columns: {', '.join(missing_cols)}", 'error')
                    return redirect(request.url)
                    
                # Convert necessary columns (handling strings like '$10' or just numbers)
                if 'Usage_Hours' in df.columns:
                    df['Usage_Hours'] = pd.to_numeric(df['Usage_Hours'], errors='coerce').fillna(0)
                if 'Cost' in df.columns:
                    df['Cost'] = pd.to_numeric(df['Cost'].astype(str).str.replace('$', '').str.replace(',', ''), errors='coerce').fillna(0)
                
                # Setup "uploads" directory
                upload_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
                os.makedirs(upload_folder, exist_ok=True)
                
                # Generate user-specific timestamp filename
                now = datetime.datetime.now()
                timestamp_str = now.strftime('%Y%m%d_%H%M%S')
                clean_filename = f"user_{session['user_id']}_{timestamp_str}.csv"
                save_path = os.path.join(upload_folder, clean_filename)
                
                # Save the validated, cleaned pandas dataframe locally
                df.to_csv(save_path, index=False)

                # Convert to dict and add user_id
                records = df.to_dict('records')
                for record in records:
                    record['user_id'] = session['user_id']
                    record['source_file'] = file.filename
                    record['upload_time'] = now
                    record['local_path'] = save_path # Reference to physical local storage
                
                # Insert records
                if records:
                    data_col.insert_many(records)
                    flash(f'Successfully uploaded and processed {len(records)} records.', 'success')
                else:
                    flash('Upload failed: No valid data found in CSV.', 'error')
                    
                return redirect(url_for('dashboard'))
                
            except Exception as e:
                flash(f'Upload failed: Error processing file - {str(e)}', 'error')
                return redirect(request.url)
        else:
            flash('Upload failed: Only CSV files are allowed.', 'error')
            
    return render_template('upload.html')

@app.route('/dashboard')
def dashboard():
    if 'user_id' not in session:
        return redirect(url_for('login'))
        
    user_data, files, selected_file = get_user_data_and_files()
    
    if not user_data:
        return render_template('dashboard.html', no_data=True, files=files, selected_file=selected_file)
        
    df = pd.DataFrame(user_data)
    
    # Check if necessary columns exist, provide fallbacks
    required_cols = ['Resource_Type', 'Region', 'Status', 'Cost', 'Usage_Hours', 'Resource_ID', 'CPU_Utilization', 'Memory_Utilization']
    for col in required_cols:
        if col not in df.columns:
            if col in ['Cost', 'Usage_Hours', 'CPU_Utilization', 'Memory_Utilization']:
                df[col] = 0.0
            else:
                df[col] = 'Unknown'

    # 1. Total Cost
    total_cost = df['Cost'].sum()
    
    # 2. Cost per resource type
    cost_per_type = df.groupby('Resource_Type')['Cost'].sum().reset_index().to_dict('records')
    
    # 3. Cost per region
    cost_per_region = df.groupby('Region')['Cost'].sum().reset_index().to_dict('records')
    
    # 4. Filter idle/stopped resources and low utilization resources
    df['Status'] = df['Status'].astype(str).str.lower()
    df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0)
    df['Usage_Hours'] = pd.to_numeric(df['Usage_Hours'], errors='coerce').fillna(0)
    df['CPU_Utilization'] = pd.to_numeric(df['CPU_Utilization'], errors='coerce').fillna(0)
    df['Memory_Utilization'] = pd.to_numeric(df['Memory_Utilization'], errors='coerce').fillna(0)
    
    idle_mask = df['Status'].isin(['stopped', 'idle', 'terminated']) & (df['Cost'] > 0)
    
    # also add low utilization to the dashboard idle_resources list for visibility
    low_util_mask = ((df['Usage_Hours'] > 0) & (df['Usage_Hours'] < 50) | 
                     (df['CPU_Utilization'] > 0) & (df['CPU_Utilization'] < 30) | 
                     (df['Memory_Utilization'] > 0) & (df['Memory_Utilization'] < 30) |
                     (df['Status'] == 'underutilized')) & (df['Cost'] > 10)
    
    high_cost_mask = df['Cost'] > 200
    
    # Combine them for the dashboard action list
    needs_action_mask = idle_mask | low_util_mask | high_cost_mask
    idle_resources = df[needs_action_mask].to_dict('records')
    
    # Optimization Suggestions & Potential Savings
    # To avoid double counting and be realistic, sum potential savings properly
    potential_savings = 0
    for item in idle_resources:
        status = str(item.get('Status', '')).lower()
        usage = float(item.get('Usage_Hours', 0))
        cost = float(item.get('Cost', 0))
        cpu = float(item.get('CPU_Utilization', 0))
        mem = float(item.get('Memory_Utilization', 0))
        
        if status in ['stopped', 'idle', 'terminated'] and cost > 0:
            potential_savings += cost
        elif (usage > 0 and usage < 50) or (cpu > 0 and cpu < 30) or (mem > 0 and mem < 30) or status == 'underutilized':
            potential_savings += cost * 0.4
        elif cost > 200:
            potential_savings += cost * 0.2
            
    # 5. Cost Trends (Line Chart)
    trend_labels = []
    trend_costs = []
    
    # Check for Date column
    date_col = next((col for col in df.columns if col.lower() == 'date'), None)
    if date_col:
        df['parsed_date'] = pd.to_datetime(df[date_col], errors='coerce').dt.date
        valid_dates_df = df.dropna(subset=['parsed_date'])
        if not valid_dates_df.empty:
            trend_data = valid_dates_df.groupby('parsed_date')['Cost'].sum().reset_index()
            trend_data = trend_data.sort_values(by='parsed_date')
            trend_labels = [str(d) for d in trend_data['parsed_date']]
            trend_costs = [str(c) for c in trend_data['Cost']]
            
    if not trend_labels and 'upload_time' in df.columns:
        df['upload_date'] = pd.to_datetime(df['upload_time']).dt.date
        trend_data = df.groupby('upload_date')['Cost'].sum().reset_index()
        trend_data = trend_data.sort_values(by='upload_date')
        trend_labels = [str(d) for d in trend_data['upload_date']]
        trend_costs = [str(c) for c in trend_data['Cost']]
        
        if len(trend_labels) == 1:
            prev_date = trend_data['upload_date'].iloc[0] - datetime.timedelta(days=1)
            trend_labels.insert(0, str(prev_date))
            trend_costs.insert(0, "0.0")
            
    # Alerts System Variables
    cost_threshold = 5000  # Default threshold
    is_over_threshold = total_cost > cost_threshold
    
    # Prepare chart data
    chart_data = {
        'types': [t['Resource_Type'] for t in cost_per_type],
        'type_costs': [t['Cost'] for t in cost_per_type],
        'regions': [r['Region'] for r in cost_per_region],
        'region_costs': [str(r['Cost']) for r in cost_per_region],
        'trend_labels': trend_labels,
        'trend_costs': trend_costs
    }
    
    recent_resources = df.tail(10).to_dict('records')

    return render_template('dashboard.html', 
                         total_cost=total_cost,
                         cost_per_type=cost_per_type,
                         cost_per_region=cost_per_region,
                         idle_resources=idle_resources,
                         potential_savings=potential_savings,
                         chart_data=chart_data,
                         recent_resources=recent_resources,
                         is_over_threshold=is_over_threshold,
                         cost_threshold=cost_threshold,
                         files=files,
                         selected_file=selected_file,
                         no_data=False)


@app.route('/download_report')
def download_report():
    if 'user_id' not in session:
        return redirect(url_for('login'))
        
    user_data, _, _ = get_user_data_and_files()
    if not user_data:
        flash('No data available to generate report', 'error')
        return redirect(url_for('dashboard'))
        
    df = pd.DataFrame(user_data)
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow(['Resource_ID', 'Resource_Type', 'Region', 'Status', 'Cost', 'Usage_Hours', 'Optimization_Suggestion'])
    
    # Provide simple suggestions based on status
    for index, row in df.iterrows():
        status = str(row.get('Status', '')).lower()
        suggestion = "Optimal"
        if status in ['idle', 'stopped']:
            suggestion = "Terminate or Downsize"
        elif float(row.get('Usage_Hours', 0)) < 10 and float(row.get('Cost', 0)) > 50:
            suggestion = "High cost vs low usage - Review needed"
            
        writer.writerow([
            row.get('Resource_ID', 'N/A'),
            row.get('Resource_Type', 'N/A'),
            row.get('Region', 'N/A'),
            row.get('Status', 'N/A'),
            row.get('Cost', '0'),
            row.get('Usage_Hours', '0'),
            suggestion
        ])
        
    output.seek(0)
    
    # Generate user-specific export filename
    now = datetime.datetime.now()
    timestamp_str = now.strftime('%Y%m%d_%H%M%S')
    export_filename = f"user_{session['user_id']}_{timestamp_str}.csv"
    
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-disposition": f"attachment; filename={export_filename}"}
    )

@app.route('/optimizations')
def optimizations():
    if 'user_id' not in session:
        return redirect(url_for('login'))
        
    user_data, files, selected_file = get_user_data_and_files()
    if not user_data:
        return render_template('optimizations.html', recommendations=[], potential_savings=0, no_data=True, files=files, selected_file=selected_file)
        
    df = pd.DataFrame(user_data)
    
    required_cols = ['Resource_Type', 'Region', 'Status', 'Cost', 'Usage_Hours', 'Resource_ID']
    for col in required_cols:
        if col not in df.columns:
            df[col] = 'Unknown' if col not in ['Cost', 'Usage_Hours'] else 0.0

    df['Status'] = df['Status'].astype(str).str.lower()
    df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0)
    df['Usage_Hours'] = pd.to_numeric(df['Usage_Hours'], errors='coerce').fillna(0)
    
    if 'CPU_Utilization' in df.columns:
        df['CPU_Utilization'] = pd.to_numeric(df['CPU_Utilization'], errors='coerce').fillna(0)
    else:
        df['CPU_Utilization'] = 0.0
        
    if 'Memory_Utilization' in df.columns:
        df['Memory_Utilization'] = pd.to_numeric(df['Memory_Utilization'], errors='coerce').fillna(0)
    else:
        df['Memory_Utilization'] = 0.0
    
    recommendations = []
    for _, row in df.iterrows():
        status = row['Status']
        usage = row['Usage_Hours']
        cost = row['Cost']
        cpu = row['CPU_Utilization']
        memory = row['Memory_Utilization']
        
        # 1. Idle / Stopped
        if status in ['stopped', 'idle', 'terminated'] and cost > 0:
            recommendations.append({
                'id': row['Resource_ID'],
                'type': row['Resource_Type'],
                'region': row['Region'],
                'issue': f"Status is '{status}' but incurring ${cost:.2f} cost.",
                'action': 'Terminate or Delete Resource immediately',
                'savings': cost
            })
            continue
            
        # 2. Underutilized - explicitly looking at CPU/Memory if available, or just Usage otherwise
        has_metrics = 'CPU_Utilization' in row or 'Memory_Utilization' in row
        is_low_cpu = has_metrics and cpu > 0 and cpu < 30
        is_low_mem = has_metrics and memory > 0 and memory < 30
        is_low_usage = usage > 0 and usage < 50 # Usage hours per month < 50 hours
        
        if (is_low_cpu or is_low_mem or is_low_usage or status == 'underutilized') and cost > 10:
            issue_desc = []
            if is_low_cpu: issue_desc.append(f"Low CPU ({cpu}%)")
            if is_low_mem: issue_desc.append(f"Low Memory ({memory}%)")
            if is_low_usage: issue_desc.append(f"Low uptime ({usage}hrs)")
            if not issue_desc: issue_desc = ["Underutilized resource"]
            
            recommendations.append({
                'id': row['Resource_ID'],
                'type': row['Resource_Type'],
                'region': row['Region'],
                'issue': f"{', '.join(issue_desc)} against high cost.",
                'action': 'Rightsize to smaller instance family or downscale',
                'savings': cost * 0.4 
            })
            continue

        # 3. High cost without massive utilization
        if cost > 200:
            recommendations.append({
                'id': row['Resource_ID'],
                'type': row['Resource_Type'],
                'region': row['Region'],
                'issue': 'Top-tier cost driver.',
                'action': 'Verify if reserved instance / savings plan can be applied',
                'savings': cost * 0.2
            })
            
    potential_savings = sum(r['savings'] for r in recommendations)

    return render_template('optimizations.html', 
                           recommendations=recommendations, 
                           potential_savings=potential_savings,
                           files=files,
                           selected_file=selected_file,
                           no_data=False)


@app.route('/reports')
def reports():
    if 'user_id' not in session:
        return redirect(url_for('login'))
        
    user_data, files, selected_file = get_user_data_and_files()
    if not user_data:
        return render_template('reports.html', summary={}, high_cost=[], no_data=True, files=files, selected_file=selected_file)
        
    df = pd.DataFrame(user_data)
    
    if 'Cost' in df.columns:
        df['Cost'] = pd.to_numeric(df['Cost'], errors='coerce').fillna(0)
    else:
        df['Cost'] = 0.0

    total_cost = df['Cost'].sum()
    resource_count = len(df)
    
    high_cost_df = df.sort_values(by='Cost', ascending=False).head(10)
    
    # Pass all resources for the detailed filterable table
    # We replace NaN with None for JSON serialization in the template or handle it with records
    df = df.where(pd.notnull(df), None)
    all_resources = df.to_dict('records')
    
    # Get distinct resource types
    resource_types = sorted([rt for rt in df['Resource_Type'].unique() if rt]) if 'Resource_Type' in df.columns else []
    
    summary = {
        'total_cost': total_cost,
        'resource_count': resource_count,
        'avg_cost': total_cost / resource_count if resource_count > 0 else 0
    }
    
    return render_template('reports.html', 
                         summary=summary, 
                         high_cost=high_cost_df.to_dict('records'), 
                         all_resources=all_resources,
                         resource_types=resource_types,
                         no_data=False, 
                         files=files, 
                         selected_file=selected_file)


@app.route('/conclusion')
def conclusion():
    if 'user_id' not in session:
        return redirect(url_for('login'))
    return render_template('conclusion.html')

# --- Admin Routes ---

@app.route('/admin')
def admin_dashboard():
    if not session.get('is_admin'):
        flash('Access denied.', 'error')
        return redirect(url_for('dashboard'))
        
    # Get all resources
    all_resources = list(data_col.find())
    
    # Calculate some quick stats
    total_cost = sum(float(r.get('Cost', 0)) for r in all_resources)
    total_users = users_col.count_documents({})
    
    return render_template('admin_dashboard.html', 
                          resources=all_resources,
                          total_cost=total_cost,
                          total_users=total_users)

@app.route('/admin/resource/add', methods=['GET', 'POST'])
def add_resource():
    if not session.get('is_admin'):
        return redirect(url_for('dashboard'))
        
    if request.method == 'POST':
        new_resource = {
            'Resource_ID': request.form['resource_id'],
            'Resource_Type': request.form['type'],
            'Region': request.form['region'],
            'Status': request.form['status'],
            'Cost': float(request.form['cost']),
            'Usage_Hours': float(request.form['usage_hours']),
            'CPU_Utilization': float(request.form.get('cpu', 0)),
            'Memory_Utilization': float(request.form.get('memory', 0)),
            'user_id': session['user_id'] # Admins can add to their own, or you could add a user dropdown
        }
        data_col.insert_one(new_resource)
        flash('Resource added successfully.', 'success')
        return redirect(url_for('admin_dashboard'))
        
    return render_template('admin_resource_form.html', action='Add')

@app.route('/admin/resource/edit/<id>', methods=['GET', 'POST'])
def edit_resource(id):
    if not session.get('is_admin'):
        return redirect(url_for('dashboard'))
        
    resource = data_col.find_one({'_id': ObjectId(id)})
    if not resource:
        flash('Resource not found', 'error')
        return redirect(url_for('admin_dashboard'))
        
    if request.method == 'POST':
        update_data = {
            'Resource_ID': request.form['resource_id'],
            'Resource_Type': request.form['type'],
            'Region': request.form['region'],
            'Status': request.form['status'],
            'Cost': float(request.form['cost']),
            'Usage_Hours': float(request.form['usage_hours']),
            'CPU_Utilization': float(request.form.get('cpu', 0)),
            'Memory_Utilization': float(request.form.get('memory', 0))
        }
        data_col.update_one({'_id': ObjectId(id)}, {'$set': update_data})
        flash('Resource updated successfully.', 'success')
        return redirect(url_for('admin_dashboard'))
        
    return render_template('admin_resource_form.html', action='Edit', resource=resource)

@app.route('/admin/resource/delete/<id>', methods=['POST'])
def delete_resource(id):
    if not session.get('is_admin'):
        return redirect(url_for('dashboard'))
        
    data_col.delete_one({'_id': ObjectId(id)})
    flash('Resource deleted.', 'success')
    return redirect(url_for('admin_dashboard'))

if __name__ == '__main__':
    app.run(debug=True, port=5000)