# -----------------------------------------------------------------------
#           Commands used to build and run the container
# 
# NOTE: Admin privileges needed (best option is execute them from Docker Desktop)
# -----------------------------------------------------------------------

# Build the Docker image (replace the next path with the one where you have stored the project)
docker build -t streamlit-imdb-dashboard "C:\Users\dfernasu\OneDrive - NTT DATA EMEAL\Documentos\GitHub\streamlit-imdb-dashboard"

# Run the Docker container
docker run -it --rm -p 8501:8501 --name streamlit-imdb-dashboard-container streamlit-imdb-dashboard