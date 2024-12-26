import React, {
  createContext,
  useReducer,
  useContext,
  useEffect,
  useCallback,
} from "react";
import { User as UserInterface } from "../pages/UsersManagementPage";

interface User {
  accessToken?: string;
  refreshToken?: string;
  profile?: UserInterface;
}

interface AuthState {
  isAuthenticated: boolean;
  user: User | null;
}

interface AuthContextProps extends AuthState {
  login: (user: User) => void;
  logout: () => void;
  updateProfile: (profile: UserInterface) => void;
}

type AuthActionType = "LOGIN" | "LOGOUT" | "UPDATE_PROFILE";

interface AuthAction {
  type: AuthActionType;
  payload?: User | UserInterface;
}

const initialState: AuthState = {
  isAuthenticated: false,
  user: null,
};

const actionTypes: { [key: string]: AuthActionType } = {
  LOGIN: "LOGIN",
  LOGOUT: "LOGOUT",
  UPDATE_PROFILE: "UPDATE_PROFILE",
};

const authReducer = (state: AuthState, action: AuthAction): AuthState => {
  switch (action.type) {
    case actionTypes.LOGIN:
      return {
        ...state,
        isAuthenticated: true,
        user: action.payload! as User,
      };
    case actionTypes.UPDATE_PROFILE:
      return {
        ...state,
        user: {
          ...state.user,
          profile: action.payload as UserInterface,
        },
      };
    case actionTypes.LOGOUT:
      return {
        ...state,
        isAuthenticated: false,
        user: null,
      };
    default:
      return state;
  }
};

const AuthContext = createContext<AuthContextProps | undefined>(undefined);

const AuthProvider = ({ children }: { children: React.ReactNode }) => {
  const [state, dispatch] = useReducer(authReducer, initialState);

  const login = useCallback((user: User) => {
    dispatch({
      type: actionTypes.LOGIN,
      payload: user,
    });
  }, []);

  const logout = useCallback(() => {
    sessionStorage.clear();
    dispatch({ type: actionTypes.LOGOUT });
  }, []);

  const updateProfile = useCallback((profile: UserInterface) => {
    dispatch({
      type: actionTypes.UPDATE_PROFILE,
      payload: profile,
    });
  }, []);

  useEffect(() => {
    const accessToken = sessionStorage.getItem("accessToken");
    const refreshToken = sessionStorage.getItem("refreshToken");
    const profile = localStorage.getItem("my-profile");

    if (accessToken && refreshToken && profile) {
      const user: User = {
        accessToken,
        refreshToken,
        profile: JSON.parse(profile),
      };

      login(user);
    }
  }, [login]);

  return (
    <AuthContext.Provider value={{ ...state, login, logout, updateProfile }}>
      {children}
    </AuthContext.Provider>
  );
};

// Create a custom hook to easily access the AuthContext values in your components
const useAuth = (): AuthContextProps => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};

export { AuthProvider, useAuth };
