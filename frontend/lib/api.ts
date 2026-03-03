import {
  Metadata,
  PlayerPrediction,
  PredictionsQueryParams,
  PredictionsResponse,
  TeamDetail,
  TeamsQueryParams,
  TeamsResponse,
} from './types';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:3000/api';

class ApiError extends Error {
  constructor(public status: number, message: string) {
    super(message);
    this.name = 'ApiError';
  }
}

async function fetchApi<T>(endpoint: string): Promise<T> {
  const url = `${API_BASE_URL}${endpoint}`;

  try {
    const response = await fetch(url);

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({ error: 'Unknown error' }));
      throw new ApiError(response.status, errorData.error || 'Request failed');
    }

    return await response.json();
  } catch (error) {
    if (error instanceof ApiError) {
      throw error;
    }
    throw new Error(`Network error: ${error instanceof Error ? error.message : 'Unknown'}`);
  }
}

function buildQueryString(params: Record<string, any>): string {
  const filtered = Object.entries(params).filter(([_, value]) => value !== undefined);
  if (filtered.length === 0) return '';

  const queryParams = new URLSearchParams();
  filtered.forEach(([key, value]) => {
    queryParams.append(key, String(value));
  });

  return `?${queryParams.toString()}`;
}

export const api = {
  predictions: {
    getAll: async (params: PredictionsQueryParams = {}): Promise<PredictionsResponse> => {
      const queryString = buildQueryString(params);
      return fetchApi<PredictionsResponse>(`/predictions${queryString}`);
    },

    getUndervalued: async (limit: number = 25): Promise<PredictionsResponse> => {
      return fetchApi<PredictionsResponse>(`/predictions/undervalued?limit=${limit}`);
    },

    getOvervalued: async (limit: number = 25): Promise<PredictionsResponse> => {
      return fetchApi<PredictionsResponse>(`/predictions/overvalued?limit=${limit}`);
    },

    getPlayer: async (playerName: string): Promise<PlayerPrediction> => {
      const encodedName = encodeURIComponent(playerName);
      return fetchApi<PlayerPrediction>(`/predictions/${encodedName}`);
    },
  },

  teams: {
    getAll: async (params: TeamsQueryParams = {}): Promise<TeamsResponse> => {
      const queryString = buildQueryString(params);
      return fetchApi<TeamsResponse>(`/teams${queryString}`);
    },

    getTeam: async (teamAbbr: string): Promise<TeamDetail> => {
      return fetchApi<TeamDetail>(`/teams/${teamAbbr}`);
    },
  },

  metadata: {
    get: async (): Promise<Metadata> => {
      return fetchApi<Metadata>('/metadata');
    },
  },
};

export { ApiError };
