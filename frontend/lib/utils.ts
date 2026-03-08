import { PlayerPrediction } from './types';

export function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function formatPercent(value: number, decimals: number = 1): string {
  return `${value.toFixed(decimals)}%`;
}

export function formatNumber(value: number, decimals: number = 1): string {
  return Number(value.toFixed(decimals)).toFixed(decimals);
}

export function getValueCategoryColor(category: 'Bargain' | 'Fair' | 'Overpaid'): string {
  const colors = {
    Bargain: 'text-white bg-green-600',
    Fair: 'text-black bg-yellow-500',
    Overpaid: 'text-white bg-red-600',
  };
  return colors[category];
}

export function getInefficiencyColor(score: number): string {
  if (score < -0.2) {
    return 'text-green-600 dark:text-green-400';
  } else if (score > 0.2) {
    return 'text-red-600 dark:text-red-400';
  }
  return 'text-yellow-600 dark:text-yellow-400';
}

export function calculateSavings(player: PlayerPrediction): number {
  return player.predicted_fmv - player.actual_salary;
}

export function formatSavings(savings: number): string {
  const formatted = formatCurrency(Math.abs(savings));
  if (savings > 0) {
    return `+${formatted}`;
  } else if (savings < 0) {
    return `-${formatted}`;
  }
  return formatted;
}

const MULTI_TEAM_INDICATORS = ['TOT', '2TM', '3TM', '4TM', '5TM'];

export function isMultiTeamIndicator(teamAbbr: string): boolean {
  return MULTI_TEAM_INDICATORS.includes(teamAbbr);
}

export function getUniquePlayersForLeaderboard(players: PlayerPrediction[]): PlayerPrediction[] {
  const playerMap = new Map<string, PlayerPrediction[]>();

  players.forEach(player => {
    const key = `${player.player_name}-${player.season}`;
    if (!playerMap.has(key)) {
      playerMap.set(key, []);
    }
    playerMap.get(key)!.push(player);
  });

  const uniquePlayers: PlayerPrediction[] = [];

  playerMap.forEach((playerRows) => {
    if (playerRows.length === 1) {
      uniquePlayers.push(playerRows[0]);
    } else {
      const aggregateRow = playerRows.find(p => isMultiTeamIndicator(p.team_abbreviation));
      if (aggregateRow) {
        uniquePlayers.push(aggregateRow);
      } else {
        uniquePlayers.push(playerRows[0]);
      }
    }
  });

  return uniquePlayers;
}

export function getPlayersCurrentTeamOnly(players: PlayerPrediction[]): PlayerPrediction[] {
  const playerMap = new Map<string, PlayerPrediction[]>();

  players.forEach(player => {
    if (isMultiTeamIndicator(player.team_abbreviation)) {
      return;
    }
    const key = `${player.player_name}-${player.season}`;
    if (!playerMap.has(key)) {
      playerMap.set(key, []);
    }
    playerMap.get(key)!.push(player);
  });

  const currentTeamPlayers: PlayerPrediction[] = [];

  playerMap.forEach((playerRows) => {
    if (playerRows.length === 1) {
      currentTeamPlayers.push(playerRows[0]);
    } else {
      currentTeamPlayers.push(playerRows[playerRows.length - 1]);
    }
  });

  return currentTeamPlayers;
}
