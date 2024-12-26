import { memo, SVGProps } from "react";

const GroupIcon = (props: SVGProps<SVGSVGElement>) => (
  <svg
    preserveAspectRatio="none"
    viewBox="0 0 7 15"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    <path
      d="M0.0625 14.375L6.9375 7.5L0.0625 0.625"
      stroke="white"
      strokeWidth={2.8}
      strokeLinecap="round"
      strokeLinejoin="round"
    />
  </svg>
);
const Memo = memo(GroupIcon);
export { Memo as GroupIcon };
