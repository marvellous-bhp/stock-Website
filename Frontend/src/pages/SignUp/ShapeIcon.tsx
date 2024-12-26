import { memo, SVGProps } from "react";

const ShapeIcon = (props: SVGProps<SVGSVGElement>) => (
  <svg
    preserveAspectRatio="none"
    viewBox="0 0 13 19"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
    {...props}
  >
    <path
      fillRule="evenodd"
      clipRule="evenodd"
      d="M2.54167 0C1.22999 0 0.166667 1.06332 0.166667 2.375V16.625C0.166667 17.9367 1.22999 19 2.54167 19H10.4583C11.77 19 12.8333 17.9367 12.8333 16.625V2.375C12.8333 1.06332 11.77 0 10.4583 0H2.54167ZM6.5 15.8333C6.93723 15.8333 7.29167 15.4789 7.29167 15.0417C7.29167 14.6044 6.93723 14.25 6.5 14.25C6.06277 14.25 5.70833 14.6044 5.70833 15.0417C5.70833 15.4789 6.06277 15.8333 6.5 15.8333Z"
      fill="black"
    />
  </svg>
);
const Memo = memo(ShapeIcon);
export { Memo as ShapeIcon };
