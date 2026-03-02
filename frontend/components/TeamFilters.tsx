'use client';

import { useState } from 'react';
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
  const [isOpen, setIsOpen] = useState(false);

  return (
    <div className="bg-cream retro-border shadow-retro p-6 mb-8 halftone-bg">
      <div
        className="flex items-center gap-3 mb-6 cursor-pointer hover:opacity-80 transition-opacity"
        onClick={() => setIsOpen(!isOpen)}
      >
        <div className="bg-retro-orange text-white px-4 py-2 subhead-retro text-sm retro-border">
          FILTERS {isOpen ? '▼' : '▶'}
        </div>
        <div className="flex-1 h-1 bg-retro-orange"></div>
      </div>

      {isOpen && (
        <div className="animate-fadeIn">
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        <div>
          <label className="block text-xs font-black uppercase tracking-wider text-gray-700  mb-2">
            Search Team
          </label>
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="Search by team name or abbreviation..."
            className="w-full px-4 py-3 retro-border bg-white  text-black  font-bold focus:outline-none focus:ring-4 focus:ring-retro-blue"
          />
        </div>

        <div>
          <label className="block text-xs font-black uppercase tracking-wider text-gray-700  mb-2">
            Sort By
          </label>
          <select
            value={sortBy}
            onChange={(e) => onSortByChange(e.target.value)}
            className="w-full px-4 py-3 retro-border bg-white  text-black  font-bold focus:outline-none focus:ring-4 focus:ring-retro-blue"
          >
            <option value="avg_inefficiency">Average Overpay %</option>
            <option value="net_efficiency">Net Overspend</option>
            <option value="bargain_count">Most Bargains</option>
            <option value="overpaid_count">Most Overpaid</option>
          </select>
        </div>
      </div>
        </div>
      )}
    </div>
  );
}
