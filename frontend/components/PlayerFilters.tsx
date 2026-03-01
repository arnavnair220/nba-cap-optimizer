'use client';

import { PredictionsQueryParams } from '@/lib/types';

interface PlayerFiltersProps {
  filters: PredictionsQueryParams;
  onFilterChange: (filters: PredictionsQueryParams) => void;
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
  { value: 'vorp', label: 'Highest VORP' },
  { value: 'predicted_fmv', label: 'Highest Predicted FMV' },
  { value: 'actual_salary', label: 'Highest Salary' },
  { value: 'player_name', label: 'Player Name (A-Z)' },
];

export default function PlayerFilters({ filters, onFilterChange }: PlayerFiltersProps) {
  const updateFilter = (key: keyof PredictionsQueryParams, value: any) => {
    const newFilters = { ...filters, [key]: value || undefined };
    if (key !== 'offset') {
      newFilters.offset = 0;
    }
    onFilterChange(newFilters);
  };

  return (
    <div className="bg-white dark:bg-gray-800 rounded-lg shadow p-6 mb-6">
      <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4">Filters</h2>

      <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
        <div>
          <label
            htmlFor="value-category"
            className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
          >
            Value Category
          </label>
          <select
            id="value-category"
            value={filters.value_category || ''}
            onChange={(e) => updateFilter('value_category', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:text-white"
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
            className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
          >
            Position
          </label>
          <select
            id="position"
            value={filters.position || ''}
            onChange={(e) => updateFilter('position', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:text-white"
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
            className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
          >
            Team
          </label>
          <input
            id="team"
            type="text"
            placeholder="e.g., LAL, GSW"
            value={filters.team || ''}
            onChange={(e) => updateFilter('team', e.target.value.toUpperCase())}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:text-white"
          />
        </div>

        <div>
          <label
            htmlFor="sort-by"
            className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-1"
          >
            Sort By
          </label>
          <select
            id="sort-by"
            value={filters.sort_by || 'inefficiency_score'}
            onChange={(e) => updateFilter('sort_by', e.target.value)}
            className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-md shadow-sm focus:ring-blue-500 focus:border-blue-500 dark:bg-gray-700 dark:text-white"
          >
            {SORT_OPTIONS.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>
      </div>

      <div className="mt-4 flex items-center justify-between">
        <div className="text-sm text-gray-600 dark:text-gray-400">
          Showing {filters.limit || 100} results
        </div>
        <button
          onClick={() =>
            onFilterChange({ sort_by: 'inefficiency_score', limit: 100, offset: 0 })
          }
          className="text-sm text-blue-600 hover:text-blue-700 dark:text-blue-400 dark:hover:text-blue-300"
        >
          Clear Filters
        </button>
      </div>
    </div>
  );
}
