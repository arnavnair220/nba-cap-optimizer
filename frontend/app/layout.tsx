import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "DUNKONOMICS - NBA Player Value Analytics",
  description: "Analyze NBA player contract efficiency with machine learning. See which players are bargains vs overpaid based on performance predictions.",
  icons: {
    icon: "/favicon.png",
  },
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en">
      <body className="antialiased">
        {children}
      </body>
    </html>
  );
}
