# NBA Cap Optimizer - Predictions API

Public REST API for querying NBA player value predictions and team efficiency metrics.

## Base URL

```
https://{api-gateway-id}.execute-api.us-east-1.amazonaws.com/v1
```

After deployment, run `terraform output api_gateway_url` to get your actual API URL.

## Authentication

No authentication required. This is a public read-only API.

## Endpoints

### 1. Get All Predictions

Get all player predictions for the current season with optional filtering.

```
GET /predictions
```

**Query Parameters:**
- `value_category` (string, optional): Filter by "Bargain", "Fair", or "Overpaid"
- `team` (string, optional): Filter by team abbreviation (e.g., "LAL", "GSW")
- `position` (string, optional): Filter by position (e.g., "PG", "SG", "SF")
- `sort_by` (string, optional): Sort field - "inefficiency_score", "predicted_fmv", "actual_salary", "vorp", "player_name" (default: "inefficiency_score")
- `limit` (integer, optional): Max results (default: 100)
- `offset` (integer, optional): Pagination offset (default: 0)

**Example:**
```bash
curl "https://{api-url}/v1/predictions?value_category=Bargain&limit=25"
```

**Response:**
```json
{
  "predictions": [
    {
      "player_name": "Jalen Brunson",
      "season": "2025-26",
      "predicted_fmv": 42000000,
      "actual_salary": 25000000,
      "predicted_salary_cap_pct": 30.4,
      "actual_salary_cap_pct": 18.1,
      "inefficiency_score": -0.40,
      "value_category": "Bargain",
      "vorp": 5.8,
      "team_abbreviation": "NYK",
      "position": "PG",
      "age": 28,
      "points": 26.5,
      "rebounds": 4.2,
      "assists": 7.8
    }
  ],
  "count": 25
}
```

---

### 2. Get Undervalued Players

Get top undervalued players (bargain contracts) sorted by inefficiency score.

```
GET /predictions/undervalued
```

**Query Parameters:**
- `limit` (integer, optional): Max results (default: 25)

**Example:**
```bash
curl "https://{api-url}/v1/predictions/undervalued?limit=10"
```

**Response:** Same format as `/predictions` endpoint.

---

### 3. Get Overvalued Players

Get top overvalued players (overpaid contracts) sorted by inefficiency score.

```
GET /predictions/overvalued
```

**Query Parameters:**
- `limit` (integer, optional): Max results (default: 25)

**Example:**
```bash
curl "https://{api-url}/v1/predictions/overvalued?limit=10"
```

**Response:** Same format as `/predictions` endpoint.

---

### 4. Get Player Prediction

Get prediction for a specific player in the current season.

```
GET /predictions/{player_name}
```

**Path Parameters:**
- `player_name` (string, required): URL-encoded player name (e.g., "LeBron%20James")

**Example:**
```bash
curl "https://{api-url}/v1/predictions/LeBron%20James"
```

**Response:**
```json
{
  "player_name": "LeBron James",
  "season": "2025-26",
  "predicted_fmv": 45000000,
  "actual_salary": 48000000,
  "predicted_salary_cap_pct": 32.6,
  "actual_salary_cap_pct": 34.8,
  "predicted_salary_pct_of_max": 92.5,
  "inefficiency_score": 0.067,
  "value_category": "Fair",
  "vorp": 4.2,
  "model_version": "v1.0.0",
  "prediction_date": "2025-02-15T10:30:00Z",
  "team_abbreviation": "LAL",
  "position": "SF",
  "age": 40,
  "games_played": 45,
  "games_started": 45,
  "minutes": 35.2,
  "points": 24.8,
  "rebounds": 7.5,
  "assists": 7.8,
  "steals": 1.2,
  "blocks": 0.6,
  "fg_pct": 0.485,
  "fg3_pct": 0.365,
  "ft_pct": 0.742,
  "per": 24.5,
  "ts_pct": 0.589,
  "usg_pct": 28.3,
  "ws": 5.2,
  "bpm": 5.8
}
```

**Error Response (404):**
```json
{
  "error": "Player 'Unknown Player' not found"
}
```

---

### 5. Get All Teams

Get team efficiency rankings with aggregated metrics.

```
GET /teams
```

**Query Parameters:**
- `sort_by` (string, optional): Sort field - "avg_inefficiency", "net_efficiency", "bargain_count", "overpaid_count" (default: "avg_inefficiency")

**Example:**
```bash
curl "https://{api-url}/v1/teams?sort_by=net_efficiency"
```

**Response:**
```json
{
  "teams": [
    {
      "team_abbreviation": "OKC",
      "full_name": "Oklahoma City Thunder",
      "player_count": 15,
      "total_payroll": 165000000,
      "avg_inefficiency_score": -0.15,
      "total_overspend": 5000000,
      "total_underspend": -30000000,
      "net_efficiency": -25000000,
      "bargain_count": 8,
      "fair_count": 5,
      "overpaid_count": 2
    }
  ],
  "count": 30
}
```

---

### 6. Get Team Detail

Get detailed team efficiency breakdown with full roster.

```
GET /teams/{team_abbreviation}
```

**Path Parameters:**
- `team_abbreviation` (string, required): Team abbreviation (e.g., "LAL", "GSW")

**Example:**
```bash
curl "https://{api-url}/v1/teams/LAL"
```

**Response:**
```json
{
  "team_abbreviation": "LAL",
  "full_name": "Los Angeles Lakers",
  "player_count": 15,
  "total_payroll": 185000000,
  "avg_inefficiency_score": 0.12,
  "total_overspend": 25000000,
  "total_underspend": -8000000,
  "net_efficiency": 17000000,
  "bargain_count": 3,
  "fair_count": 8,
  "overpaid_count": 4,
  "players": [
    {
      "player_name": "LeBron James",
      "predicted_fmv": 45000000,
      "actual_salary": 48000000,
      "predicted_salary_cap_pct": 32.6,
      "actual_salary_cap_pct": 34.8,
      "inefficiency_score": 0.067,
      "value_category": "Fair",
      "vorp": 4.2,
      "position": "SF",
      "age": 40,
      "points": 24.8,
      "rebounds": 7.5,
      "assists": 7.8
    }
  ]
}
```

**Error Response (404):**
```json
{
  "error": "Team 'ABC' not found"
}
```

---

## Response Fields

### Prediction Fields

| Field | Type | Description |
|-------|------|-------------|
| `player_name` | string | Player's full name |
| `season` | string | NBA season (YYYY-YY format) |
| `predicted_fmv` | integer | Predicted Fair Market Value in dollars |
| `actual_salary` | integer | Actual annual salary in dollars |
| `predicted_salary_cap_pct` | float | Predicted salary as % of salary cap |
| `actual_salary_cap_pct` | float | Actual salary as % of salary cap |
| `inefficiency_score` | float | (actual - predicted) / predicted. Negative = underpaid, Positive = overpaid |
| `value_category` | string | "Bargain" (< -0.20), "Fair" (-0.20 to 0.20), or "Overpaid" (> 0.20) |
| `vorp` | float | Value Over Replacement Player |
| `team_abbreviation` | string | Team abbreviation |
| `position` | string | Player position |

### Team Efficiency Fields

| Field | Type | Description |
|-------|------|-------------|
| `avg_inefficiency_score` | float | Average inefficiency across roster |
| `total_overspend` | integer | Sum of dollars overspent on overvalued players |
| `total_underspend` | integer | Sum of dollars saved on undervalued players (negative number) |
| `net_efficiency` | integer | Net over/under spending (total_overspend + total_underspend) |
| `bargain_count` | integer | Number of bargain contracts on roster |
| `fair_count` | integer | Number of fair contracts on roster |
| `overpaid_count` | integer | Number of overpaid contracts on roster |

---

## CORS

CORS is enabled for all origins (`*`). The API supports preflight OPTIONS requests.

---

## Error Responses

### 400 Bad Request
```json
{
  "error": "Player name required"
}
```

### 404 Not Found
```json
{
  "error": "Player 'Unknown Player' not found"
}
```

### 500 Internal Server Error
```json
{
  "error": "Database connection failed"
}
```

---

## Rate Limiting

Currently no rate limiting is enforced. This may be added in the future.

---

## Example Frontend Usage

### JavaScript/Fetch
```javascript
const API_BASE_URL = 'https://{api-url}/v1';

// Get undervalued players
async function getUndervaluedPlayers() {
  const response = await fetch(`${API_BASE_URL}/predictions/undervalued?limit=25`);
  const data = await response.json();
  return data.predictions;
}

// Get specific player
async function getPlayer(playerName) {
  const encodedName = encodeURIComponent(playerName);
  const response = await fetch(`${API_BASE_URL}/predictions/${encodedName}`);
  if (!response.ok) {
    throw new Error(`Player not found: ${playerName}`);
  }
  return await response.json();
}

// Get team detail
async function getTeam(teamAbbr) {
  const response = await fetch(`${API_BASE_URL}/teams/${teamAbbr}`);
  return await response.json();
}
```

### React Hook Example
```javascript
import { useState, useEffect } from 'react';

function useUndervaluedPlayers() {
  const [players, setPlayers] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`${API_BASE_URL}/predictions/undervalued?limit=25`)
      .then(res => res.json())
      .then(data => {
        setPlayers(data.predictions);
        setLoading(false);
      });
  }, []);

  return { players, loading };
}
```

---

## Deployment

The API is deployed using Terraform:

```bash
cd infrastructure/terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

After deployment, get your API URL:

```bash
terraform output api_gateway_url
```

---

## Local Testing

To test the Lambda handler locally:

```bash
cd src/api
python handler.py
```

This will run a test event and print the response.
