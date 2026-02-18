# NBA Salary Cap Optimizer

An end-to-end MLOps platform that predicts Fair Market Value (FMV) for NBA players using machine learning, identifying market inefficiencies by comparing predictions to actual salaries.

## Overview

This system analyzes NBA player performance data to predict what players should be paid based on their statistics, age, position, and historical market trends. By comparing these predictions to actual contracts, it identifies undervalued and overvalued players across the league.

**Key Components:**
- Automated daily data pipeline from NBA APIs and web sources
- ML models (XGBoost, LightGBM) for FMV prediction
- REST API serving predictions and rankings
- Interactive web dashboard for analysis
- AWS cloud infrastructure (Lambda, SageMaker, RDS, S3)