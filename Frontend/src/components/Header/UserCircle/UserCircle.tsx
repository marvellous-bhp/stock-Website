import { memo } from "react";
import type { FC } from "react";

import resets from "../../_resets.module.css";
import { Union1Icon } from "./Union1Icon.js";
import classes from "./UserCircle.module.css";

interface Props {
  className?: string;
  classes?: {
    root?: string;
  };
  onClick?: () => void;
}

export const UserCircle: FC<Props> = memo(function UserCircle(props) {
  return (
    <div
      className={`${resets.storybrainResets} ${props.classes?.root || ""} ${
        props.className || ""
      } ${classes.root}`}
    >
      <button className={classes.union1} onClick={props.onClick}>
        <Union1Icon className={classes.icon} />
      </button>
    </div>
  );
});
