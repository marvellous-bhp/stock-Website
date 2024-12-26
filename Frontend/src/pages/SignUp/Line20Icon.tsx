import { memo, SVGProps } from "react";

const Line20Icon = (props: SVGProps<SVGSVGElement>) => (
  <svg
    preserveAspectRatio="none"
    viewBox="0 0 871 1"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    <path opacity={0.05} d="M0 0H871" stroke="#FBFBFB" />
  </svg>
);
const Memo = memo(Line20Icon);
export { Memo as Line20Icon };
