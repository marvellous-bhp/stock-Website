import { memo } from "react";
import type { FC, ReactNode } from "react";

import resets from "../../_resets.module.css";
import classes from "./EnvelopeLightSolid.module.css";
import { ShapeIcon } from "./ShapeIcon.js";

interface Props {
  className?: string;
  classes?: {
    root?: string;
  };
  swap?: {
    shape?: ReactNode;
  };
}

export const EnvelopeLightSolid: FC<Props> = memo(function EnvelopeLightSolid(
  props = {}
) {
  return (
    <div
      className={`${resets.storybrainResets} ${props.classes?.root || ""} ${
        props.className || ""
      } ${classes.root}`}
    >
      <div className={classes.shape}>
        {props.swap?.shape || <ShapeIcon className={classes.icon} />}
      </div>
    </div>
  );
});
