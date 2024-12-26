import classes from "./SearchBox.module.css";
import SearchIcon from "@mui/icons-material/Search";
const SearchBox = () => {
  return (
    <div className={classes.searchContainer}>
      <input
        type="text"
        id="searchInput"
        className={classes.search}
        placeholder="search..."
      />
      <button className={classes.btnsearch} onClick={handleSearch}>
        <SearchIcon />
      </button>
    </div>
  );
};

const handleSearch = () => {};

export default SearchBox;
