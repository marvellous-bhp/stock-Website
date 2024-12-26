import { memo } from "react";
import type { FC, ReactNode } from "react";

import resets from "../_resets.module.css";
import classes from "./ChevronRight2.module.css";
import { GroupIcon } from "./GroupIcon.js";

interface Props {
  className?: string;
  classes?: {
    group?: string;
    root?: string;
  };
  swap?: {
    group?: ReactNode;
  };
}
export const ChevronRight2: FC<Props> = memo(function ChevronRight2(
  props = {}
) {
  return (
    <div
      className={`${resets.storybrainResets} ${props.classes?.root || ""} ${
        props.className || ""
      } ${classes.root}`}
    >
      <div className={`${props.classes?.group || ""} ${classes.group}`}>
        {props.swap?.group || <GroupIcon className={classes.icon} />}
      </div>
    </div>
  );
});
