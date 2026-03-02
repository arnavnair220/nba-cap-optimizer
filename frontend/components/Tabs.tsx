'use client';

import { ReactNode } from 'react';

interface Tab {
  id: string;
  label: string;
  icon?: string;
}

interface TabsProps {
  tabs: Tab[];
  activeTab: string;
  onTabChange: (tabId: string) => void;
}

export default function Tabs({ tabs, activeTab, onTabChange }: TabsProps) {
  return (
    <div className="mb-8">
      <div className="flex gap-2 overflow-x-auto pt-1">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            onClick={() => onTabChange(tab.id)}
            className={`
              relative px-8 py-4 font-black uppercase tracking-wide subhead-retro text-lg
              transition-all retro-border
              ${
                activeTab === tab.id
                  ? 'bg-retro-red text-white shadow-retro transform -translate-y-1'
                  : 'bg-cream text-black hover:bg-gray-100'
              }
            `}
          >
            {tab.icon && <span className="mr-2">{tab.icon}</span>}
            {tab.label}
            {activeTab === tab.id && (
              <div className="absolute bottom-0 left-0 right-0 h-1 bg-retro-blue"></div>
            )}
          </button>
        ))}
      </div>
      <div className="h-1 bg-black -mt-px"></div>
    </div>
  );
}

interface TabPanelProps {
  children: ReactNode;
  tabId: string;
  activeTab: string;
}

export function TabPanel({ children, tabId, activeTab }: TabPanelProps) {
  if (tabId !== activeTab) return null;

  return <div className="animate-fadeIn">{children}</div>;
}
