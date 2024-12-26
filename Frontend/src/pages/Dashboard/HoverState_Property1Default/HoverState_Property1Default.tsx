import { memo } from "react";
import type { FC, ReactNode } from "react";

import resets from "../../../components/_resets.module.css";
import classes from "./HoverState_Property1Default.module.css";

interface Props {
  className?: string;
  text?: {
    item?: ReactNode;
  };
}

export const HoverState_Property1Default: FC<Props> = memo(
  function HoverState_Property1Default(props = {}) {
    return (
      <div className={`${resets.storybrainResets} ${classes.root}`}>
        {props.text?.item != null ? (
          props.text?.item
        ) : (
          <div className={classes.item}>Item</div>
        )}
      </div>
    );
  }
);
