import type { ReactNode } from "react";
import "./style.css";
import "@xyflow/react/dist/style.css";

export default function Layout({ children }: { children: ReactNode }) {
  return <html lang="en"><body>{children}</body></html>;
}
