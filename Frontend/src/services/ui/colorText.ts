export const setColorText = async (xText: number): Promise<string> => {
  let textColor = "white";

  if (xText > 0) {
    textColor = "green";
  } else if (xText < 0) {
    textColor = "red";
  } else {
    textColor = "yellow";
  }
  return textColor;
};
