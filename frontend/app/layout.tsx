import type { Metadata } from "next";
import "./globals.css";

export const metadata: Metadata = {
  title: "NBA Cap Optimizer - Player Value Dashboard",
  description: "Interactive dashboard ranking NBA players by fair market value predictions",
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
