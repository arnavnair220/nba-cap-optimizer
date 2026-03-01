import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./pages/**/*.{js,ts,jsx,tsx,mdx}",
    "./components/**/*.{js,ts,jsx,tsx,mdx}",
    "./app/**/*.{js,ts,jsx,tsx,mdx}",
  ],
  theme: {
    extend: {
      colors: {
        background: "var(--background)",
        foreground: "var(--foreground)",
        court: {
          wood: "#C19A6B",
          line: "#FFFFFF",
          key: "#CC0000",
        },
        retro: {
          black: "#000000",
          white: "#FFFFFF",
          red: "#E31837",
          blue: "#006BB6",
          yellow: "#FFC72C",
          orange: "#FF6B35",
        },
      },
      fontFamily: {
        display: ["Impact", "Arial Black", "sans-serif"],
        sport: ["Arial Black", "Arial", "sans-serif"],
      },
      boxShadow: {
        retro: "4px 4px 0px 0px rgba(0, 0, 0, 1)",
        "retro-lg": "8px 8px 0px 0px rgba(0, 0, 0, 1)",
      },
      backgroundImage: {
        'retro-stripes': "repeating-linear-gradient(45deg, rgba(0,0,0,0.03) 0px, rgba(0,0,0,0.03) 2px, transparent 2px, transparent 4px)",
        'retro-bold-stripes': "repeating-linear-gradient(45deg, transparent, transparent 35px, rgba(0,0,0,0.04) 35px, rgba(0,0,0,0.04) 70px)",
        'halftone': "url(\"data:image/svg+xml,%3Csvg width='20' height='20' viewBox='0 0 20 20' xmlns='http://www.w3.org/2000/svg'%3E%3Cg fill='%23000000' fill-opacity='0.05'%3E%3Ccircle cx='3' cy='3' r='3'/%3E%3Ccircle cx='13' cy='13' r='3'/%3E%3C/g%3E%3C/svg%3E\")",
      },
    },
  },
  plugins: [],
};
export default config;
