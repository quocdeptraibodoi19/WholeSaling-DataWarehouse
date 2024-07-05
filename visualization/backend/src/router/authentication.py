import os
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))

from fastapi import (
    APIRouter,
    Response,
    status,
    Path,
    Query,
    Depends,
    Body,
    HTTPException,
)
from fastapi.security import OAuth2PasswordRequestForm, OAuth2PasswordBearer

from core.connections import OperationalDBConnection
from models.user import AccountUser, DBUser, User, Token

import traceback

from datetime import datetime, timedelta

from passlib.context import CryptContext
from jose import JWTError, jwt

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.getenv("ACCESS_TOKEN_EXPIRE_MINUTES"))

router = APIRouter(prefix="/authentication", tags=["authentication"])

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/authentication/login")


def get_password_hash(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return pwd_context.verify(plain_password, hashed_password)


def create_access_token(data: dict, expires_delta: timedelta = None):
    to_encode = data.copy()
    expire = (
        datetime.now() + expires_delta
        if expires_delta
        else datetime.now() + timedelta(minutes=15)
    )
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


@router.post("/create")
def create_user(user: AccountUser):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        connection.autocommit = False

        transaction_time = datetime.now()
        hashed_password = pwd_context.hash(user.password)

        insert_query = OperationalDBConnection.get_postgres_sql(
            """
                INSERT INTO users (first_name, last_name, user_name, password, created_at, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
        )
        cursor.execute(
            insert_query,
            (
                user.first_name,
                user.last_name,
                user.user_name,
                hashed_password,
                transaction_time,
                transaction_time,
            ),
        )
        connection.commit()

    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return Response(status_code=status.HTTP_200_OK, content="User created successfully")


@router.post("/login", response_model=Token)
def login(login_form: OAuth2PasswordRequestForm = Depends()):
    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        connection = db_connection.connect()
        cursor = connection.cursor()

        query = OperationalDBConnection.get_postgres_sql(
            """
                SELECT user_id, first_name, last_name, user_name, password
                FROM users
                WHERE user_name = %s
            """
        )
        cursor.execute(query, (login_form.username,))
        result = cursor.fetchone()

        db_user = None
        if result:
            user_id, first_name, last_name, user_name, hashed_password = result
            db_user = DBUser(
                user_name=user_name,
                user_id=user_id,
                first_name=first_name,
                last_name=last_name,
                password=hashed_password,
            )

        if not db_user or not verify_password(login_form.password, db_user.password):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Incorrect username or password",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
        access_token = create_access_token(
            data={"sub": db_user.user_name}, expires_delta=access_token_expires
        )
    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return {"access_token": access_token, "token_type": "bearer"}


def get_current_user(token: str = Depends(oauth2_scheme)):
    credentials_exception = HTTPException(
        status_code=401,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    db_connection = OperationalDBConnection()
    connection = db_connection.connect()
    try:
        cursor = connection.cursor()
        query = OperationalDBConnection.get_postgres_sql(
            """
                SELECT user_id, first_name, last_name FROM users WHERE user_name = %s
            """
        )
        cursor.execute(query, (username,))
        result = cursor.fetchone()

        if result:
            user_id, first_name, last_name = result
            user = User(
                user_name=username,
                first_name=first_name,
                last_name=last_name,
                user_id=user_id,
            )
        else:
            raise credentials_exception

    except Exception:
        print(traceback.format_exc())
        raise (Exception)
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()

    return user
