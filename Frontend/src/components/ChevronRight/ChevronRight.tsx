import { memo } from "react";
import type { FC } from "react";

import resets from "../_resets.module.css";
import { ChevronRight2 } from "../ChevronRight2/ChevronRight2";
import classes from "./ChevronRight.module.css";
import { GroupIcon } from "./GroupIcon.js";

interface Props {
  className?: string;
}
export const ChevronRight: FC<Props> = memo(function ChevronRight(props = {}) {
  return (
    <div className={`${resets.storybrainResets} ${classes.root}`}>
      <ChevronRight2
        className={classes.chevronRight}
        classes={{ group: classes.group }}
        swap={{
          group: (
            <div className={classes.group}>
              <GroupIcon className={classes.icon} />
            </div>
          ),
        }}
      />
    </div>
  );
});
