import { memo } from "react";
import type { FC } from "react";
import resets from "../../../components/_resets.module.css";
import classes from "./InterfaceEssentialLock_StyleFi.module.css";
import { StyleFilledIcon } from "./StyleFilledIcon.js";

interface Props {
  className?: string;
  classes?: {
    root?: string;
  };
}
export const InterfaceEssentialLock_StyleFi: FC<Props> = memo(
  function InterfaceEssentialLock_StyleFi(props = {}) {
    return (
      <div
        className={`${resets.storybrainResets} ${props.classes?.root || ""} ${
          props.className || ""
        } ${classes.root}`}
      >
        <div className={classes.icon}>
          <StyleFilledIcon className={classes.icon2} />
        </div>
      </div>
    );
  }
);
