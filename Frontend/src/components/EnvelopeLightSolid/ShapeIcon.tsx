import { memo, SVGProps } from 'react';

const ShapeIcon = (props: SVGProps<SVGSVGElement>) => (
  <svg preserveAspectRatio='none' viewBox='0 0 22 18' fill='none' xmlns='http://www.w3.org/2000/svg' {...props}>
    <path
      fillRule='evenodd'
      clipRule='evenodd'
      d='M3 0C1.34315 0 0 1.34315 0 3V15C0 16.6569 1.34315 18 3 18H19C20.6569 18 22 16.6569 22 15V3C22 1.34315 20.6569 0 19 0H3ZM5.64021 4.2318C5.21593 3.87824 4.58537 3.93556 4.2318 4.35984C3.87824 4.78412 3.93556 5.41468 4.35984 5.76825L9.07947 9.70127C10.192 10.6284 11.808 10.6284 12.9206 9.70127L17.6402 5.76825C18.0645 5.41468 18.1218 4.78412 17.7682 4.35984C17.4147 3.93556 16.7841 3.87824 16.3598 4.2318L11.6402 8.16483C11.2694 8.47387 10.7307 8.47387 10.3598 8.16483L5.64021 4.2318Z'
      fill='#121319'
    />
  </svg>
);
const Memo = memo(ShapeIcon);
export { Memo as ShapeIcon };
