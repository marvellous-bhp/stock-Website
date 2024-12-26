import { memo, SVGProps } from "react";

const ShapeIcon2 = (props: SVGProps<SVGSVGElement>) => (
  <svg
    preserveAspectRatio="none"
    viewBox="0 0 20 16"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M3.33333 0.5C1.95262 0.5 0.833333 1.61929 0.833333 3V13C0.833333 14.3807 1.95262 15.5 3.33333 15.5H16.6667C18.0474 15.5 19.1667 14.3807 19.1667 13V3C19.1667 1.61929 18.0474 0.5 16.6667 0.5H3.33333ZM5.53351 4.0265C5.17994 3.73187 4.65447 3.77964 4.35984 4.1332C4.0652 4.48676 4.11297 5.01224 4.46653 5.30687L8.39956 8.58439C9.32668 9.35699 10.6734 9.35699 11.6005 8.58439L15.5335 5.30687C15.8871 5.01224 15.9348 4.48676 15.6402 4.1332C15.3456 3.77964 14.8201 3.73187 14.4665 4.0265L10.5335 7.30402C10.2245 7.56156 9.77557 7.56156 9.46653 7.30403L5.53351 4.0265Z"
      fill="black"
    />
  </svg>
);
const Memo = memo(ShapeIcon2);
export { Memo as ShapeIcon2 };
