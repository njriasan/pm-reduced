# If the conda binary is not found, specify the full path to it
# you can find it by searching for "conda" under the miniconda3 directory
# typical paths are:
# - on linux: /home/<user>/miniconda3/bin/conda
# - on OSX: /Users/<user>/miniconda3/bin/conda
# - on Windows: C:/Users/<user>/Miniconda3/Scripts/conda


echo "Setting up blank environment"
conda create --name emission-pm python=3.6

echo "Updating using conda now"
conda env update --name emission-pm --file setup/environment36.yml
echo "Initializing using conda now"
conda init bash
