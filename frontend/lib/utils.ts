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
  return value.toFixed(decimals);
}

export function getValueCategoryColor(category: 'Bargain' | 'Fair' | 'Overpaid'): string {
  const colors = {
    Bargain: 'text-white bg-green-600',
    Fair: 'text-black bg-retro-yellow',
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
