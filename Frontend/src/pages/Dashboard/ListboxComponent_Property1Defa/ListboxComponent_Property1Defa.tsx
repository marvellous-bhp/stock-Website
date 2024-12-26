import * as React from "react";
import { memo } from "react";
import type { FC } from "react";
import FormControl from "@mui/material/FormControl";
import MenuItem from "@mui/material/MenuItem";
import InputLabel from "@mui/material/InputLabel";
import Select, { SelectChangeEvent } from "@mui/material/Select";
import { Theme, useTheme } from "@mui/material/styles";

interface Props {
  className?: string;
  classes?: {
    root?: string;
  };
}

export const ListboxComponent_Property1Defa: FC<Props> = memo(
  function ListboxComponent_Property1Defa(props = {}) {
    const [bank, setBank] = React.useState("");

    const handleChange = (event: SelectChangeEvent) => {
      setBank(event.target.value);
    };

    return (
      <div>
        <FormControl
          variant="standard"
          sx={{
            m: 1,
            minWidth: 120,
            backgroundColor: "#fff",
            borderRadius: "4px",
          }}
        >
          <InputLabel id="demo-simple-select-standard-label">Bank</InputLabel>
          <Select
            labelId="demo-simple-select-standard-label"
            id="demo-simple-select-standard"
            value={bank}
            onChange={handleChange}
            label="Age"
          >
            <MenuItem value="">
              <em>None</em>
            </MenuItem>
            <MenuItem value={10}>ACB</MenuItem>
            <MenuItem value={20}>BIDV</MenuItem>
            <MenuItem value={30}>VIB</MenuItem>
          </Select>
        </FormControl>
      </div>
    );
  }
);
