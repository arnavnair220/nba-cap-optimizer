'use client';

import { PredictionsQueryParams } from '@/lib/types';

interface PlayerFiltersProps {
  filters: PredictionsQueryParams;
  onFilterChange: (filters: PredictionsQueryParams) => void;
  searchQuery: string;
  onSearchChange: (query: string) => void;
  team: string;
  onTeamChange: (team: string) => void;
  position: string;
  onPositionChange: (position: string) => void;
  valueCategory: string;
  onValueCategoryChange: (category: string) => void;
  sortBy: string;
  onSortByChange: (sortBy: string) => void;
  resultCount: number;
}

const POSITIONS = ['PG', 'SG', 'SF', 'PF', 'C'];

const VALUE_CATEGORIES = [
  { value: '', label: 'All Players' },
  { value: 'Bargain', label: 'Bargain Contracts' },
  { value: 'Fair', label: 'Fair Contracts' },
  { value: 'Overpaid', label: 'Overpaid Contracts' },
];

const SORT_OPTIONS = [
  { value: 'inefficiency_score', label: 'Best Value (Default)' },
  { value: 'worst_value', label: 'Worst Value' },
  { value: 'predicted_fmv', label: 'Highest Predicted FMV' },
  { value: 'actual_salary', label: 'Highest Salary' },
  { value: 'player_name', label: 'Player Name (A-Z)' },
];

export default function PlayerFilters({
  filters,
  onFilterChange,
  searchQuery,
  onSearchChange,
  team,
  onTeamChange,
  position,
  onPositionChange,
  valueCategory,
  onValueCategoryChange,
  sortBy,
  onSortByChange,
  resultCount,
}: PlayerFiltersProps) {
  const updateFilter = (key: keyof PredictionsQueryParams, value: any) => {
    const newFilters = { ...filters, [key]: value || undefined };
    if (key !== 'offset') {
      newFilters.offset = 0;
    }
    onFilterChange(newFilters);
  };

  return (
    <div className="bg-white dark:bg-gray-900 retro-border shadow-retro p-6 mb-8 halftone-bg">
      <div className="flex items-center gap-3 mb-6">
        <div className="bg-retro-orange text-white px-4 py-2 subhead-retro text-sm retro-border">
          FILTERS
        </div>
        <div className="flex-1 h-1 bg-retro-orange"></div>
      </div>

      <div className="mb-4">
        <label
          htmlFor="player-search"
          className="block text-xs font-bold text-black dark:text-white mb-2 uppercase tracking-wide"
        >
          Search Player
        </label>
        <input
          id="player-search"
          type="text"
          placeholder="Enter player name..."
          value={searchQuery}
          onChange={(e) => onSearchChange(e.target.value)}
          className="w-full px-3 py-2 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold placeholder:text-gray-500 focus:ring-2 focus:ring-retro-blue"
        />
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div>
          <label
            htmlFor="value-category"
            className="block text-xs font-bold text-black dark:text-white mb-2 uppercase tracking-wide"
          >
            Value Category
          </label>
          <select
            id="value-category"
            value={valueCategory}
            onChange={(e) => onValueCategoryChange(e.target.value)}
            className="w-full px-3 py-2 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold focus:ring-2 focus:ring-retro-blue"
          >
            {VALUE_CATEGORIES.map((cat) => (
              <option key={cat.value} value={cat.value}>
                {cat.label}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label
            htmlFor="position"
            className="block text-xs font-bold text-black dark:text-white mb-2 uppercase tracking-wide"
          >
            Position
          </label>
          <select
            id="position"
            value={position}
            onChange={(e) => onPositionChange(e.target.value)}
            className="w-full px-3 py-2 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold focus:ring-2 focus:ring-retro-blue"
          >
            <option value="">All Positions</option>
            {POSITIONS.map((pos) => (
              <option key={pos} value={pos}>
                {pos}
              </option>
            ))}
          </select>
        </div>

        <div>
          <label
            htmlFor="team"
            className="block text-xs font-bold text-black dark:text-white mb-2 uppercase tracking-wide"
          >
            Team
          </label>
          <input
            id="team"
            type="text"
            placeholder="e.g., LAL, GSW"
            value={team}
            onChange={(e) => onTeamChange(e.target.value.toUpperCase())}
            className="w-full px-3 py-2 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold placeholder:text-gray-500 focus:ring-2 focus:ring-retro-blue"
          />
        </div>

        <div>
          <label
            htmlFor="sort-by"
            className="block text-xs font-bold text-black dark:text-white mb-2 uppercase tracking-wide"
          >
            Sort By
          </label>
          <select
            id="sort-by"
            value={sortBy}
            onChange={(e) => onSortByChange(e.target.value)}
            className="w-full px-3 py-2 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold focus:ring-2 focus:ring-retro-blue"
          >
            {SORT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="mt-6 flex items-center justify-between">
        <div className="text-sm font-bold text-black dark:text-white uppercase">
          Showing {resultCount} results
        </div>
        <button
          onClick={() => {
            onFilterChange({ limit: 500, offset: 0 });
            onSearchChange('');
            onTeamChange('');
            onPositionChange('');
            onValueCategoryChange('');
            onSortByChange('inefficiency_score');
          }}
          className="bg-black text-white px-4 py-2 retro-border font-bold uppercase text-sm hover:bg-gray-800 transition-colors"
        >
          Clear All
        </button>
      </div>
    </div>
  );
}
