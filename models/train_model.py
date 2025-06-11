import pandas as pd
import numpy as np
import pickle
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.metrics import accuracy_score, f1_score

# Paths
DATA_PATH = r"D:\EU School\Academics\3rd Semester\Business Intelligence\Sharleen\E-Commerce ETL\Data\Processed\cleaned_data.csv"
MODEL_SAVE_PATH = r"D:\EU School\Academics\3rd Semester\Business Intelligence\Sharleen\E-Commerce ETL\models\model.pkl"

# Load data
df = pd.read_csv(DATA_PATH)

# Target: IsHighRated = 1 if rating >= 4
df['IsHighRated'] = np.where(df['Rating'] >= 4, 1, 0)

# Features
features = ['Price', 'Discount', 'StockQuantity', 'NumReviews', 'Sales']
X = df[features].fillna(0)
y = df['IsHighRated']

# Split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Scale features
scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)
X_test_scaled = scaler.transform(X_test)

# Train final model (KNN)
model = KNeighborsClassifier()
model.fit(X_train_scaled, y_train)

# Evaluate
y_pred = model.predict(X_test_scaled)
acc = accuracy_score(y_test, y_pred)
f1 = f1_score(y_test, y_pred)

print(f"📊 Final KNN — Accuracy: {acc:.3f}, F1 Score: {f1:.3f}")

# Save the model
with open(MODEL_SAVE_PATH, 'wb') as f:
    pickle.dump(model, f)

print(f"✅ Final KNN model saved to: {MODEL_SAVE_PATH}")
