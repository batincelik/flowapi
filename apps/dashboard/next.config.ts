import type { NextConfig } from "next";
const apiOrigin=process.env.FLOWAPI_API_ORIGIN||"http://localhost:8000";
const config:NextConfig={async rewrites(){return [{source:"/api/:path*",destination:`${apiOrigin}/api/:path*`},{source:"/hooks/:path*",destination:`${apiOrigin}/hooks/:path*`}];}};
export default config;
