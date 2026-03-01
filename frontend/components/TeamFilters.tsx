'use client';

import { TeamsQueryParams } from '@/lib/types';

interface TeamFiltersProps {
  sortBy: string;
  onSortByChange: (sortBy: string) => void;
  searchQuery: string;
  onSearchChange: (query: string) => void;
}

export default function TeamFilters({
  sortBy,
  onSortByChange,
  searchQuery,
  onSearchChange,
}: TeamFiltersProps) {
  return (
    <div className="bg-white dark:bg-gray-900 retro-border shadow-retro p-6 mb-8 halftone-bg">
      <div className="flex items-center gap-3 mb-6">
        <div className="bg-retro-orange text-white px-4 py-2 subhead-retro text-sm retro-border">
          FILTERS
        </div>
        <div className="flex-1 h-1 bg-retro-orange"></div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div>
          <label className="block text-xs font-black uppercase tracking-wider text-gray-700 dark:text-gray-300 mb-2">
            Search Team
          </label>
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Search by team name or abbreviation..."
            className="w-full px-4 py-3 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold focus:outline-none focus:ring-4 focus:ring-retro-blue"
          />
        </div>

        <div>
          <label className="block text-xs font-black uppercase tracking-wider text-gray-700 dark:text-gray-300 mb-2">
            Sort By
          </label>
          <select
            value={sortBy}
            onChange={(e) => onSortByChange(e.target.value)}
            className="w-full px-4 py-3 retro-border bg-white dark:bg-gray-800 text-black dark:text-white font-bold focus:outline-none focus:ring-4 focus:ring-retro-blue"
          >
            <option value="avg_inefficiency">Average Efficiency</option>
            <option value="net_efficiency">Net Efficiency</option>
            <option value="bargain_count">Most Bargains</option>
            <option value="overpaid_count">Most Overpaid</option>
          </select>
        </div>
      </div>
    </div>
  );
}
