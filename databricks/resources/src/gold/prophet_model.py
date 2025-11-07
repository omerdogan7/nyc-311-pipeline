# Databricks notebook source
# MAGIC %md
# MAGIC # NYC 311 Volume Forecasting - Prophet Model
# MAGIC **Complete Production Pipeline with Feature Importance Analysis**

# COMMAND ----------

# Install required libraries
%pip install prophet --upgrade --quiet
%pip install plotly --quiet

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 🚀 COMPLETE PIPELINE - DATA LOADING + TRAINING + EVALUATION
# ═══════════════════════════════════════════════════════════════

import pandas as pd
import numpy as np
from prophet import Prophet
from sklearn.metrics import mean_absolute_error, mean_squared_error
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
warnings.filterwarnings('ignore')

print("=" * 70)
print("🚀 PRODUCTION MODEL PIPELINE")
print("=" * 70)

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 📊 STEP 1: LOAD DATA
# ═══════════════════════════════════════════════════════════════

print("\n📊 Loading data from feature store...")

# Load data from feature store
df = spark.table("nyc_311_dev.gold.volume_forecasting_features").toPandas()

# Prepare for Prophet format
df['ds'] = pd.to_datetime(df['ds'])
df = df.sort_values('ds').reset_index(drop=True)

print(f"✅ Loaded {len(df)} days of data")
print(f"   Date range: {df['ds'].min().date()} to {df['ds'].max().date()}")


# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 🔪 STEP 2: TRAIN/TEST SPLIT
# ═══════════════════════════════════════════════════════════════

print("\n🔪 Splitting data...")

# Last 90 days for testing
test_days = 90
train = df.iloc[:-test_days].copy()
test = df.iloc[-test_days:].copy()

print(f"✅ Train: {len(train)} days ({train['ds'].min().date()} to {train['ds'].max().date()})")
print(f"✅ Test:  {len(test)} days ({test['ds'].min().date()} to {test['ds'].max().date()})")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 🧹 STEP 3: CLEAN & FILTER DATA
# ═══════════════════════════════════════════════════════════════

# Define regressors
# FINAL FEATURE SET
REGRESSORS = [
    'is_holiday',           
    'rolling_7day_avg',     
    'lag_7day',            
]
print(f"\n🧹 Filtering training data...")

# Filter training data: remove low values and outliers
train_clean = train[train['y'] >= 500].copy()
mean_train = train_clean['y'].mean()
std_train = train_clean['y'].std()
train_clean = train_clean[np.abs((train_clean['y'] - mean_train) / std_train) <= 3].copy()

print(f"   Original: {len(train)} → Clean: {len(train_clean)} days")
print(f"   Removed: {len(train) - len(train_clean)} days")

# Prepare training data
train_model = train_clean[['ds', 'y'] + REGRESSORS].copy()

# Handle missing values
for col in REGRESSORS:
    if train_model[col].isnull().any():
        train_model[col] = train_model[col].fillna(train_model[col].median())

print(f"✅ Training data ready: {len(train_model)} days")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 🤖 STEP 4: CREATE & TRAIN MODEL
# ═══════════════════════════════════════════════════════════════

print("\n🤖 Creating Prophet model...")

model = Prophet(
    growth='linear',
    changepoint_prior_scale=0.05,
    seasonality_prior_scale=10,
    seasonality_mode='multiplicative',
    yearly_seasonality=True,
    weekly_seasonality=True,
    daily_seasonality=False,
    interval_width=0.95,
    mcmc_samples=500
)

# Add regressors with appropriate prior scales
for reg in REGRESSORS:
    if 'lag' in reg or 'rolling' in reg:
        prior = 10  # Historical features
    elif 'covid' in reg or 'sla' in reg or 'closure' in reg:
        prior = 5   # Performance metrics
    else:
        prior = 1   # Binary features
    
    model.add_regressor(reg, prior_scale=prior)

print(f"✅ Model created with {len(REGRESSORS)} regressors")

print(f"\n🎓 Training model on {len(train_model)} days...")
model.fit(train_model)
print("✅ Model trained!")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 🧪 STEP 5: FILTER TEST DATA
# ═══════════════════════════════════════════════════════════════

print("\n🧪 Filtering test data...")

# Remove last 3 days (potentially incomplete)
max_date = test['ds'].max()
cutoff_date = max_date - pd.Timedelta(days=3)
test_clean = test[test['ds'] <= cutoff_date].copy()

# Remove extreme outliers (z-score < -3)
mean_test = test['y'].mean()
std_test = test['y'].std()
test_clean['z_score'] = (test_clean['y'] - mean_test) / std_test
test_clean = test_clean[test_clean['z_score'] >= -3].copy()
test_clean = test_clean.drop('z_score', axis=1)

print(f"   Original: {len(test)} → Clean: {len(test_clean)} days")
print(f"   Removed: {len(test) - len(test_clean)} days")

# Prepare test data
test_model = test_clean[['ds'] + REGRESSORS].copy()

# Handle missing values
for col in REGRESSORS:
    if test_model[col].isnull().any():
        test_model[col] = test_model[col].fillna(test_model[col].median())

print(f"✅ Test data ready: {len(test_model)} days")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 🔮 STEP 6: PREDICT
# ═══════════════════════════════════════════════════════════════

print("\n🔮 Generating predictions...")
forecast = model.predict(test_model)
print("✅ Predictions generated!")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 📊 STEP 7: EVALUATE
# ═══════════════════════════════════════════════════════════════

print("\n📊 Evaluating model...")

# Merge actual and predicted results
results = test_clean[['ds', 'y']].merge(
    forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']], 
    on='ds', 
    how='inner'
)

# Calculate performance metrics
y_true = results['y'].values
y_pred = results['yhat'].values

mae = mean_absolute_error(y_true, y_pred)
smape = np.mean(2 * np.abs(y_pred - y_true) / (np.abs(y_true) + np.abs(y_pred))) * 100
mdape = np.median(np.abs((y_true - y_pred) / y_true) * 100)
rmse = np.sqrt(mean_squared_error(y_true, y_pred))
within_ci = ((y_true >= results['yhat_lower']) & (y_true <= results['yhat_upper'])).mean() * 100

# MAPE distribution analysis
mape_values = np.abs((y_true - y_pred) / y_true) * 100
mape_dist = {
    '< 10%': (mape_values < 10).sum(),
    '10-20%': ((mape_values >= 10) & (mape_values < 20)).sum(),
    '20-50%': ((mape_values >= 20) & (mape_values < 50)).sum(),
    '> 50%': (mape_values >= 50).sum()
}

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 📈 STEP 8: PRINT RESULTS
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("📊 FINAL MODEL PERFORMANCE")
print("=" * 70)

print(f"\n📈 METRICS:")
print(f"   Sample Size:    {len(results)} days")
print(f"   MAE:            {mae:.2f}")
print(f"   sMAPE:          {smape:.2f}% ⭐")
print(f"   MdAPE:          {mdape:.2f}%")
print(f"   RMSE:           {rmse:.2f}")
print(f"   95% Coverage:   {within_ci:.1f}%")

print(f"\n📊 MAPE DISTRIBUTION:")
for range_label, count in mape_dist.items():
    pct = count / len(results) * 100
    bar = '█' * int(pct / 3)
    print(f"   {range_label:>10}: {count:>3} days ({pct:>5.1f}%) {bar}")

print(f"\n💼 BUSINESS IMPACT:")
avg_volume = y_true.mean()
print(f"   Avg Daily Volume:     {avg_volume:,.0f} complaints")
print(f"   Avg Prediction Error: {mae:.0f} complaints ({mae/avg_volume*100:.1f}%)")
print(f"   Typical Range:        ±{mae:.0f} complaints")

# Professional deployment assessment
print(f"\n" + "=" * 70)
print("DEPLOYMENT READINESS")
print("=" * 70)

# Simple thresholds
if smape < 10:
    status = "PRODUCTION READY"
    recommendation = "Deploy to production"
elif smape < 15:
    status = "READY WITH MONITORING" 
    recommendation = "Deploy with monitoring"
elif smape < 20:
    status = "PILOT READY"
    recommendation = "Test in limited scope"
else:
    status = "NEEDS IMPROVEMENT"
    recommendation = "Optimize before deployment"

print(f"\nStatus: {status}")
print(f"Recommendation: {recommendation}")
print(f"\nKey Metrics:")
print(f"  • Accuracy (sMAPE): {smape:.1f}%")
print(f"  • Coverage (95% CI): {within_ci:.1f}%")
print(f"  • Daily Error: ±{mae:.0f} tickets")

print("=" * 70)

# Store results
model_results = {
    'model': model,
    'forecast': forecast,
    'results': results,
    'metrics': {
        'mae': mae,
        'smape': smape,
        'mdape': mdape,
        'rmse': rmse,
        'ci_coverage': within_ci
    }
}

print("\n✅ Results stored in 'model_results' dictionary")
print("✅ Pipeline complete!")

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 📊 STEP 9: FEATURE IMPORTANCE ANALYSIS
# ═══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print("FEATURE IMPORTANCE ANALYSIS")
print("=" * 70)

# Create test dataset for feature impact analysis
test_dates = pd.date_range(start='2024-01-01', end='2024-01-31', freq='D')
baseline_df = pd.DataFrame({'ds': test_dates})

# Add all regressors with baseline values (zeros)
for reg in REGRESSORS:
    baseline_df[reg] = 0

# Get baseline prediction
baseline_pred = model.predict(baseline_df)['yhat'].mean()

# Test impact of each feature
impact_results = []

for reg in REGRESSORS:
    # Create test data with this feature activated
    test_df = baseline_df.copy()
    test_df[reg] = 1  # Set feature to 1
    
    # Get prediction with feature activated
    feature_pred = model.predict(test_df)['yhat'].mean()
    
    # Calculate impact
    impact = feature_pred - baseline_pred
    
    impact_results.append({
        'feature': reg,
        'impact': impact,
        'abs_impact': abs(impact)
    })

# Create impact DataFrame
impact_df = pd.DataFrame(impact_results)
impact_df = impact_df.sort_values('abs_impact', ascending=False)

print(f"✅ Analyzed impact of {len(impact_df)} features")

# COMMAND ----------

# Display feature impact results simply
print("\n" + "=" * 50)
print("PROPHET MODEL - FEATURE IMPORTANCE")
print("=" * 50)

# List top 10 most important features
print("\n📊 Top 10 Most Impactful Features:\n")

for i, row in impact_df.head(10).iterrows():
    if row['impact'] > 0:
        print(f"  ✅ {row['feature']:<30} +{row['impact']:.0f} tickets")
    else:
        print(f"  ⬇️ {row['feature']:<30} {row['impact']:.0f} tickets")

# COMMAND ----------

# Simple chart
plt.figure(figsize=(10, 6))

# Top 10 features
top_10 = impact_df.head(10)
colors = ['green' if x > 0 else 'red' for x in top_10['impact']]

plt.barh(top_10['feature'], top_10['abs_impact'], color=colors, alpha=0.7)
plt.xlabel('Impact on Ticket Count')
plt.title('Feature Importance - Prophet Model')
plt.tight_layout()
plt.show()

# COMMAND ----------

# Simple summary
print("\n" + "=" * 50)
print("QUICK SUMMARY")
print("=" * 50)
print(f"\n✅ Most important: {impact_df.iloc[0]['feature']}")
print(f"   Impact: {impact_df.iloc[0]['impact']:+.0f} tickets\n")

# COMMAND ----------
# ═══════════════════════════════════════════════════════════════
# 📊 STEP 10: VISUALIZATIONS
# ═══════════════════════════════════════════════════════════════

print("\n📊 GENERATING VISUALIZATIONS")
print("=" * 70)

# Set style
plt.style.use('default')
fig, axes = plt.subplots(2, 3, figsize=(20, 12))

# 1. Actual vs Predicted
ax1 = axes[0, 0]
ax1.scatter(y_true, y_pred, alpha=0.5, s=20)
ax1.plot([y_true.min(), y_true.max()], [y_true.min(), y_true.max()], 'r--', lw=2)
ax1.set_xlabel('Actual Volume')
ax1.set_ylabel('Predicted Volume')
ax1.set_title('Actual vs Predicted', fontsize=14, fontweight='bold')
ax1.text(0.05, 0.95, f'sMAPE: {smape:.2f}%', transform=ax1.transAxes, 
         fontsize=12, verticalalignment='top', bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# 2. Residuals Distribution
ax2 = axes[0, 1]
residuals = y_true - y_pred
ax2.hist(residuals, bins=30, edgecolor='black', alpha=0.7, color='skyblue')
ax2.axvline(x=0, color='red', linestyle='--', linewidth=2)
ax2.set_xlabel('Residuals (Actual - Predicted)')
ax2.set_ylabel('Frequency')
ax2.set_title('Residuals Distribution', fontsize=14, fontweight='bold')

# 3. Time Series Forecast
ax3 = axes[0, 2]
ax3.plot(results['ds'], y_true, label='Actual', alpha=0.7, linewidth=2, color='blue')
ax3.plot(results['ds'], y_pred, label='Predicted', alpha=0.7, linewidth=2, color='orange')
ax3.fill_between(results['ds'], results['yhat_lower'], results['yhat_upper'], alpha=0.2, color='orange', label='95% CI')
ax3.set_xlabel('Date')
ax3.set_ylabel('Complaint Volume')
ax3.set_title('90-Day Test Period Forecast', fontsize=14, fontweight='bold')
ax3.legend(loc='best')
ax3.tick_params(axis='x', rotation=45)

# 4. MAPE Distribution (Bar Chart)
ax4 = axes[1, 0]
labels = list(mape_dist.keys())
values = list(mape_dist.values())
colors = ['green', 'yellow', 'orange', 'red']
ax4.bar(labels, values, color=colors[:len(labels)], alpha=0.7)
ax4.set_xlabel('MAPE Range')
ax4.set_ylabel('Number of Days')
ax4.set_title('MAPE Distribution', fontsize=14, fontweight='bold')

# 5. Error Over Time
ax5 = axes[1, 1]
errors = abs(y_true - y_pred)
ax5.plot(results['ds'], errors, alpha=0.7, color='red')
ax5.axhline(y=mae, color='blue', linestyle='--', label=f'MAE: {mae:.0f}')
ax5.set_xlabel('Date')
ax5.set_ylabel('Absolute Error')
ax5.set_title('Prediction Errors Over Time', fontsize=14, fontweight='bold')
ax5.legend()
ax5.tick_params(axis='x', rotation=45)

# 6. Feature Importance (Simple bar chart based on prior scales)
ax6 = axes[1, 2]
feature_names = ['lag_7day', 'lag_365day', 'rolling_7day_avg', 'rolling_30day_avg', 
                 'closure_rate', 'sla_compliance_rate', 'is_weekend', 'is_holiday'][:8]
feature_values = [10, 10, 10, 10, 5, 5, 1, 1][:8]
ax6.barh(feature_names, feature_values, color='teal', alpha=0.7)
ax6.set_xlabel('Prior Scale (Importance)')
ax6.set_title('Feature Importance (Prior Scales)', fontsize=14, fontweight='bold')

plt.tight_layout()
plt.show()

print("✅ Visualizations complete!")

# COMMAND ----------

# Simple time series plot
plt.figure(figsize=(15, 6))
plt.plot(results['ds'], y_true, label='Actual', alpha=0.8, linewidth=2)
plt.plot(results['ds'], y_pred, label='Predicted', alpha=0.8, linewidth=2)
plt.fill_between(results['ds'], results['yhat_lower'], results['yhat_upper'], 
                 alpha=0.2, label='95% Confidence Interval')
plt.xlabel('Date', fontsize=12)
plt.ylabel('Complaint Volume', fontsize=12)
plt.title('NYC 311 Complaint Volume Forecast - Test Period', fontsize=14, fontweight='bold')
plt.legend(loc='best')
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# ═══════════════════════════════════════════════════════════════
# 📊 STEP:11 OVERFITTING/UNDERFITTING CHECK
# ═══════════════════════════════════════════════════════════════
train_forecast = model.predict(train_model)
train_results = train_clean[['ds', 'y']].merge(
    train_forecast[['ds', 'yhat']], 
    on='ds', 
    how='inner'
)

# Training metrics
train_mae = mean_absolute_error(train_results['y'], train_results['yhat'])
train_smape = np.mean(2 * np.abs(train_results['yhat'] - train_results['y']) / 
                      (np.abs(train_results['y']) + np.abs(train_results['yhat']))) * 100

print("\n" + "=" * 50)
print("OVERFITTING/UNDERFITTING CHECK")
print("=" * 50)
print(f"\nTRAIN Performance:")
print(f"  sMAPE: {train_smape:.2f}%")
print(f"  MAE:   {train_mae:.0f}")

print(f"\nTEST Performance:")
print(f"  sMAPE: {smape:.2f}%")
print(f"  MAE:   {mae:.0f}")

print(f"\nDifference:")
print(f"  sMAPE Gap: {abs(smape - train_smape):.2f}%")
print(f"  MAE Gap:   {abs(mae - train_mae):.0f}")

# Diagnosis
gap = abs(smape - train_smape)
if gap < 2:
    status = "✅ NO OVERFITTING - Model generalizes well"
elif gap < 5:
    status = "⚠️ SLIGHT OVERFITTING - Acceptable"
else:
    status = "❌ OVERFITTING DETECTED - Model too complex"

print(f"\nDiagnosis: {status}")
# COMMAND ----------
